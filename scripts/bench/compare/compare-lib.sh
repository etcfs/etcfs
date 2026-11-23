#!/bin/bash
# compare-lib.sh — shared provisioning for the etcfs-vs-juicefs-vs-gluster-vs-nfs
# comparison suite (scripts/bench/compare/bench-*.sh).
#
# Each bench-*.sh in this directory owns a fully isolated 3-node cluster + one
# io2 Multi-Attach EBS volume (1000 IOPS by default), created and destroyed by
# itself — never shared with another backend's run, unlike scripts/bench/'s
# other scenarios which all reuse one already-up etcfs cluster. That isolation
# is what create-infra.sh/destroy-infra.sh already give for free (both are
# backend-agnostic: they provision compute + a raw EBS volume, nothing
# etcfs-specific), driven through a private ETCFS_STATE file per backend.
#
# Requires: scripts/infra/create-infra.sh, scripts/infra/destroy-infra.sh.
# Source this after setting COMPARE_BACKEND (e.g. "juicefs") — it derives an
# isolated state file and cluster name from it.

COMPARE_BACKEND="${COMPARE_BACKEND:?set COMPARE_BACKEND before sourcing compare-lib.sh}"
COMPARE_SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPARE_PROJECT_ROOT="$(cd "$COMPARE_SCRIPT_DIR/../../.." && pwd)"
INFRA_DIR="$COMPARE_PROJECT_ROOT/scripts/infra"

# The backend a variant benchmarks: "etcfs-pagecache" is still etcfs as far as
# AMI choice, mounting and teardown go — only its results directory differs.
COMPARE_BACKEND_BASE="${COMPARE_BACKEND%%-*}"

# gfs2-utils/dlm/corosync and glusterfs-server exist on AL2 and not on AL2023
# (each script's own header explains why); pinned here rather than in five
# callers so a scenario script only has to name a backend.
case "$COMPARE_BACKEND_BASE" in
    gfs2|gluster) export ETCFS_AMI_NAME_FILTER="${ETCFS_AMI_NAME_FILTER:-amzn2-ami-hvm-*-x86_64-gp2}" ;;
esac

# One cluster name and state file per run, not per backend. Teardown is
# asynchronous, so the next run of the same backend can start while the last
# one is still being deleted; with a shared name it adopted the old run's
# security group by name moments before teardown deleted it (RunInstances then
# fails with InvalidGroup.NotFound), and the old teardown's final rm of the
# state file deleted the new run's.
COMPARE_RUN_ID="${COMPARE_RUN_ID:-$(date +%Y%m%d-%H%M%S)-$$}"
export ETCFS_STATE="$COMPARE_PROJECT_ROOT/infra-state-compare-${COMPARE_BACKEND}-${COMPARE_RUN_ID}.json"
export ETCFS_CLUSTER="compare-${COMPARE_BACKEND}-${COMPARE_RUN_ID}"
export ETCFS_COMPUTE_NODES="${ETCFS_COMPUTE_NODES:-3}"
export ETCFS_VOLUME_IOPS="${ETCFS_VOLUME_IOPS:-1000}"
export ETCFS_VOLUME_SIZE="${ETCFS_VOLUME_SIZE:-20}"

source "$INFRA_DIR/state.sh"
source "$COMPARE_SCRIPT_DIR/../bench-lib.sh"

# Not $PROJECT_ROOT: state.sh derives that from $0, which here is bench-*.sh
# (three directories deeper than scripts/infra/*.sh, where state.sh assumes
# it's invoked from), so it resolves one level too high once state.sh is
# sourced from this directory instead of run as a top-level script itself.
RESULTS_DIR="$COMPARE_PROJECT_ROOT/benchmark-results/compare/$COMPARE_BACKEND"
mkdir -p "$RESULTS_DIR"

# compare_client_nodes <wanted> — provision enough nodes for <wanted> of them to
# end up with the filesystem actually mounted, and export that count before
# compare_provision reads it. gluster and juicefs spend node0 on the server (the
# first brick / redis + object store) and never mount it there, so a scenario
# that needs N clients needs N+1 nodes from them and N from everyone else.
# Without this a three-node sweep silently becomes a two-node one for those two
# backends, and the curves stop being comparable at the point that matters.
compare_client_nodes() {
    local want="$1"
    case "$COMPARE_BACKEND_BASE" in
        gluster|juicefs) export ETCFS_COMPUTE_NODES="$((want + 1))" ;;
        *)               export ETCFS_COMPUTE_NODES="$want" ;;
    esac
}

compare_provision() {
    log "=== [$COMPARE_BACKEND] provisioning 3-node cluster + ${ETCFS_VOLUME_IOPS}-IOPS Multi-Attach volume ==="
    bash "$INFRA_DIR/create-infra.sh"
    mapfile -t COMPARE_PUB_IPS < <(state_get compute_public_ips | jq -r '.[]')
    mapfile -t COMPARE_PRIV_IPS < <(state_get compute_ips | jq -r '.[]')
    COMPARE_VOL_ID=$(state_get volume_id | tr -d '"')
    for ip in "${COMPARE_PUB_IPS[@]}"; do
        wait_for_ssh "$ip" 40 5 || die "compare_provision: $ip never came up over SSH"
    done
}

# compare_destroy — always run from a `trap ... EXIT`. Fire-and-forget, same
# pattern as scripts/test/chaos-lib.sh's own teardown_cluster: nohup+disown so
# the calling bench-*.sh's own exit isn't gated on instance termination /
# volume deletion actually completing, which is what lets independent
# backends run back-to-back (or in parallel, in separate shells) without one
# backend's teardown blocking the next backend's provisioning. destroy-infra.sh
# underneath is still the same synchronous, confirms-every-step script — this
# just moves the wait off the critical path, it doesn't skip it.
compare_destroy() {
    log "=== [$COMPARE_BACKEND] tearing down (async) ==="
    # The lock descriptor is explicitly closed in the child: teardown outlives
    # this script by design, and a copy of the descriptor keeps the run lock
    # held, so the next run in a serial queue is refused by a cluster that is
    # already being deleted.
    nohup bash "$INFRA_DIR/destroy-infra.sh" --force \
        > "$COMPARE_PROJECT_ROOT/teardown-${COMPARE_BACKEND}-${COMPARE_RUN_ID}.log" 2>&1 \
        {COMPARE_LOCK_FD}>&- &
    disown
}

# compare_begin [extra_teardown] — install the teardown trap and provision.
# Every bench-*.sh opens with this pair; extra_teardown is a command run before
# compare_destroy, for a backend that created resources of its own.
# ---- run guardrails ----
#
# Two mistakes cost most of a day and manufactured a 1.54x "regression" that
# did not exist: several clusters benchmarking at once on one account, and a
# burstable instance whose CPU credits were exhausted for the whole run. The
# run lock below prevents the first; state.sh refuses burstable types outright.

COMPARE_RUN_LOCK="${COMPARE_RUN_LOCK:-$COMPARE_PROJECT_ROOT/.compare-run.lock}"
# Defined even when the lock is never taken (COMPARE_ALLOW_CONCURRENT=1), because
# compare_destroy closes this descriptor in the teardown child unconditionally.
COMPARE_LOCK_FD=9

# compare_take_run_lock — refuse to start while another comparison run holds
# the lock. Set COMPARE_ALLOW_CONCURRENT=1 to run two on purpose (they will
# contend for the same account's instance and volume limits, and neither
# result is comparable with a run that had the account to itself).
compare_take_run_lock() {
    [[ "${COMPARE_ALLOW_CONCURRENT:-0}" == "1" ]] && return 0
    # Appended, not truncated: opening for write would erase the holder line of
    # the run this one is about to be refused by, and the refusal would then
    # name nobody.
    exec {COMPARE_LOCK_FD}>>"$COMPARE_RUN_LOCK"
    if ! flock -n "$COMPARE_LOCK_FD"; then
        die "another comparison run is in flight (holder: $(cat "$COMPARE_RUN_LOCK" 2>/dev/null)).
     Runs must be serial: concurrent clusters on one account distort every
     number. Wait for it, or set COMPARE_ALLOW_CONCURRENT=1 to override."
    fi
    : > "$COMPARE_RUN_LOCK"
    printf '%s pid=%s started=%s\n' "$COMPARE_BACKEND" "$$" "$(date -Is)" >&"$COMPARE_LOCK_FD"
}

compare_begin() {
    local extra="${1:-}"
    # gluster always leaves per-node local volumes behind, whichever scenario
    # drove it — the caller shouldn't have to remember that.
    [[ -z "$extra" && "$COMPARE_BACKEND_BASE" == "gluster" ]] && extra=compare_destroy_local_volumes
    compare_take_run_lock
    if [[ -n "$extra" ]]; then
        # Expanded now on purpose: extra is the caller's literal command, and
        # the local holding it is gone by the time the trap fires.
        # shellcheck disable=SC2064
        trap "$extra; compare_destroy" EXIT
    else
        trap "compare_destroy" EXIT
    fi
    compare_provision
}

# compare_install_fio <pub_ip>... — install fio wherever the run will drive it.
# dnf or yum: this suite spans AL2023 and AL2, and the backends that need AL2
# say so in their own headers.
compare_install_fio() {
    local ip
    for ip in "$@"; do
        $SSH_CMD "ec2-user@$ip" \
            "sudo dnf install -y fio >/dev/null 2>&1 || sudo yum install -y fio >/dev/null 2>&1"
    done
}

# compare_shared_device <pub_ip> — the cluster's Multi-Attach volume as seen by
# that node, failing loudly rather than letting a later mkfs run against an
# empty path.
compare_shared_device() {
    local dev
    dev=$(detect_ebs_dev "$1")
    [[ -n "$dev" ]] || die "$COMPARE_BACKEND: no shared EBS device found on $1"
    echo "$dev"
}

# compare_finish <label> — record the run's summary row and say where it went.
compare_finish() {
    compare_summary_row "$1" "$RESULTS_DIR/$1.json"
    log "$1 comparison run complete. Results in $RESULTS_DIR"
}

# compare_export_backing <server_pub_ip> <server_priv_ip> <client_pub_ips...>
# — format the shared Multi-Attach volume as ext4 on the server node only
# (Multi-Attach guarantees safe concurrent *block* access, not a concurrent
# filesystem — mounting ext4 from two nodes at once corrupts it) and NFS-export
# it to the rest of the cluster, mounted at an identical path everywhere.
#
# nfs backend benchmarks this mount directly. gluster and juicefs use it as
# their shared backing store instead of local per-node disks, because a
# single 1000-IOPS Multi-Attach volume — the resource shape this suite was
# asked to give every backend — has no per-node-local storage to hand out;
# an NFS re-export is the closest same-shaped substitute, not how either
# product would be deployed in production. Their own reports say so.
# compare_open_port <from_port> [to_port] — self-referencing SG ingress rule,
# same pattern as create-infra.sh's own etcd rules. create-infra.sh only opens
# etcd's own ports (2379/2380) plus 9090/9100 — every backend here needs its
# own protocol's port opened before its first cross-node connection, or that
# connection hangs until TCP's own timeout instead of failing fast.
compare_open_port() {
    local from="$1" to="${2:-$1}"
    local sg; sg=$(state_get sg_id | tr -d '"')
    aws ec2 authorize-security-group-ingress --group-id "$sg" \
        --protocol tcp --port "${from}-${to}" --source-group "$sg" >/dev/null 2>&1 || true
}

# compare_open_port_udp — same, for corosync's totem transport (5405/udp).
compare_open_port_udp() {
    local from="$1" to="${2:-$1}"
    local sg; sg=$(state_get sg_id | tr -d '"')
    aws ec2 authorize-security-group-ingress --group-id "$sg" \
        --protocol udp --port "${from}-${to}" --source-group "$sg" >/dev/null 2>&1 || true
}

BACKING_PATH=/mnt/compare-backing
compare_export_backing() {
    local server_pub="$1" server_priv="$2"
    shift 2
    local client_pubs=("$@")

    compare_open_port 2049
    local dev
    dev=$(detect_ebs_dev "$server_pub")
    [[ -n "$dev" ]] || die "compare_export_backing: no EBS device found on $server_pub"

    $SSH_CMD "ec2-user@$server_pub" "
        sudo mkfs.ext4 -q -F $dev
        sudo mkdir -p $BACKING_PATH
        sudo mount $dev $BACKING_PATH
        sudo dnf install -y nfs-utils >/dev/null 2>&1 || sudo yum install -y nfs-utils >/dev/null 2>&1
        echo '$BACKING_PATH *(rw,sync,no_subtree_check,no_root_squash)' | sudo tee /etc/exports >/dev/null
        sudo systemctl enable --now nfs-server >/dev/null 2>&1
        sudo exportfs -ra
    "
    for ip in "${client_pubs[@]}"; do
        $SSH_CMD "ec2-user@$ip" "
            sudo dnf install -y nfs-utils >/dev/null 2>&1 || sudo yum install -y nfs-utils >/dev/null 2>&1
            sudo mkdir -p $BACKING_PATH
            sudo mount -t nfs4 $server_priv:$BACKING_PATH $BACKING_PATH
        "
    done
}

# compare_create_local_volumes <iops> <pub_ip>:<priv_ip>:<instance_id> ...
# — one independent io2 volume per node, attached and formatted as that
# node's own local disk. This is for backends that replicate across
# independent per-node storage (Gluster) rather than one device shared by
# every node (etcfs's raw Multi-Attach volume, or NFS/JuiceFS's single
# server-side disk behind a network protocol) — giving them the *shared*
# volume instead would mean either corrupting it (concurrent ext4 mounts on
# one Multi-Attach device) or relaying every brick through NFS, which is not
# how the product is deployed and would benchmark the relay, not the product.
# Prints one device path per node, in input order. Every volume is tagged
# ClusterName=$ETCFS_CLUSTER,Name=<cluster>-local so compare_destroy_local_volumes
# can find them again by tag instead of by an in-memory list — this function
# is normally called as `mapfile ... < <(compare_create_local_volumes ...)`,
# and process substitution runs the function in a subshell, so any plain
# variable/array it set would never be visible to the caller once it returns.
compare_create_local_volumes() {
    local iops="$1"
    shift
    local entry pub inst vol_id dev
    for entry in "$@"; do
        IFS=: read -r pub _ inst <<< "$entry"
        vol_id=$(aws ec2 create-volume --volume-type io2 --size "$ETCFS_VOLUME_SIZE" \
            --iops "$iops" --availability-zone "$AZ" \
            --tag-specifications "ResourceType=volume,Tags=[{Key=ClusterName,Value=$ETCFS_CLUSTER},{Key=Name,Value=${ETCFS_CLUSTER}-local}]" \
            --query 'VolumeId' --output text)
        aws ec2 wait volume-available --volume-ids "$vol_id" 2>/dev/null || true
        aws ec2 attach-volume --volume-id "$vol_id" --instance-id "$inst" --device /dev/sdg >/dev/null
        aws ec2 wait volume-in-use --volume-ids "$vol_id" 2>/dev/null || true
        # Retried: "volume-in-use" is the attachment's AWS-side state, not a
        # guarantee the guest kernel has enumerated the device node yet — at
        # higher node counts (several attaches firing close together) that lag
        # is long enough for a single-shot check to miss it and leave the
        # caller's devs[] short an entry.
        dev=""
        for _ in 1 2 3 4 5 6 7 8 9 10; do
            dev=$($SSH_CMD "ec2-user@$pub" "for d in /dev/nvme2n1 /dev/sdg /dev/xvdg; do [[ -b \$d ]] && echo \$d && break; done")
            [[ -n "$dev" ]] && break
            sleep 3
        done
        [[ -n "$dev" ]] || die "compare_create_local_volumes: device not visible on $pub"
        echo "$dev"
    done
}

# compare_destroy_local_volumes — sweeps by tag rather than an in-memory list,
# so it also catches volumes from an earlier attempt of this same backend
# that failed before reaching its own teardown (this suite's iterate-until-
# green workflow leaves exactly that behind otherwise — every failed retry's
# local volumes, orphaned and still billing). Async, same as compare_destroy:
# force-detach (--force, no wait for "available") and delete immediately —
# a delete that lands before the detach is fully processed just fails and
# retries a few times in the background, off the caller's critical path.
compare_destroy_local_volumes() {
    (
        for vol_id in $(aws ec2 describe-volumes \
            --filters "Name=tag:ClusterName,Values=$ETCFS_CLUSTER" "Name=tag:Name,Values=${ETCFS_CLUSTER}-local" \
            --query 'Volumes[].VolumeId' --output text 2>/dev/null); do
            aws ec2 detach-volume --volume-id "$vol_id" --force >/dev/null 2>&1 || true
            for _ in 1 2 3 4 5; do
                aws ec2 delete-volume --volume-id "$vol_id" >/dev/null 2>&1 && break
                sleep 5
            done
        done
    ) > "$COMPARE_PROJECT_ROOT/teardown-${COMPARE_BACKEND}-${COMPARE_RUN_ID}-local-volumes.log" 2>&1 &
    disown
}

# compare_summary_row <label> <fio_json> — appends one row to this backend's
# own summary.json (an array of {label, write_iops, write_p99_us, read_iops,
# read_p99_us}), the shape report.sh reads back across every backend.

# compare_p99_us <json> <job_index> <read|write> — the 99th percentile of
# completion latency, in microseconds, from either fio JSON dialect.
#
# fio 3.x reports `clat_ns.percentile` in nanoseconds; fio 2.14 — which is what
# the AL2 AMI that gfs2 and gluster need still ships — reports
# `clat.percentile` in microseconds and has no `clat_ns` at all. Reading only
# the 3.x key made every p99 from those two backends come back as 0, which
# earlier reports published as "missing data" rather than as the real figure it
# was sitting next to.
compare_p99_us() {
    jq -r --argjson j "$2" --arg d "$3" '
        (.jobs[$j][$d].clat_ns.percentile."99.000000") as $ns
        | if $ns then ($ns / 1000 | round)
          else ((.jobs[$j][$d].clat.percentile."99.000000" // 0) | round) end' "$1"
}

compare_summary_row() {
    local label="$1" file="$2"
    local row
    row=$(jq --arg l "$label" \
        --argjson wp "$(compare_p99_us "$file" 0 write)" \
        --argjson rp "$(compare_p99_us "$file" 1 read)" '{
        label: $l,
        write_iops: (.jobs[0].write.iops // 0 | round),
        write_p99_us: $wp,
        read_iops: (.jobs[1].read.iops // 0 | round),
        read_p99_us: $rp
    }' "$file")
    local summary="$RESULTS_DIR/summary.json"
    [[ -f "$summary" ]] || echo "[]" > "$summary"
    jq --argjson r "$row" '. + [$r]' "$summary" > "$summary.tmp" && mv "$summary.tmp" "$summary"
}

# ---- headline numbers ----
#
# The scenario suite reports one number per scenario, not an IOPS table, so it
# needs a sink that isn't fio-shaped. compare_headline appends to this
# backend's headline.json (an array of {scenario, backend, metric, value,
# unit}) and prints the number where the run's own log will show it.
compare_headline() {
    local scenario="$1" metric="$2" value="$3" unit="${4:-}"
    local file="$RESULTS_DIR/headline.json"
    [[ -f "$file" ]] || echo "[]" > "$file"
    jq --arg s "$scenario" --arg b "$COMPARE_BACKEND" --arg m "$metric" \
       --arg v "$value" --arg u "$unit" \
       '. + [{scenario: $s, backend: $b, metric: $m, value: ($v | tonumber), unit: $u}]' \
       "$file" > "$file.tmp" && mv "$file.tmp" "$file"
    log "HEADLINE [$COMPARE_BACKEND/$scenario] $metric = $value $unit"
}

# ---- generic remote timing ----
#
# Timed on the node itself, not around the ssh call: an ssh round trip is tens
# to hundreds of milliseconds, which is the same order as several of the
# numbers this suite exists to publish (recovery, join latency).
compare_remote_time() {
    local ip="$1"; shift
    $SSH_CMD "ec2-user@$ip" "s=\$(date +%s.%N); { $*; } >/dev/null 2>&1; e=\$(date +%s.%N); awk -v s=\$s -v e=\$e 'BEGIN{printf \"%.3f\", e-s}'"
}

compare_epoch() { date +%s.%N; }

# compare_etcfs_read_ops <pub_ip> — the daemon's own count of READ operations
# served, from its Prometheus endpoint. Empty for a non-etcfs backend, which has
# no such counter; callers treat that as "not measurable" rather than zero.
compare_etcfs_read_ops() {
    [[ "$COMPARE_BACKEND_BASE" == "etcfs" ]] || return 0
    $SSH_CMD "ec2-user@$1" \
        "curl -s --max-time 5 http://127.0.0.1:9090/metrics | awk '/^etcfuse_fuse_ops_total\{op=\"read\"\}/ {printf \"%d\", \$2}'" 2>/dev/null
}

# compare_etcfs_snapshot_metrics <pub_ip> <label> — save the daemon's whole
# Prometheus page to the results directory, so a scenario can take one before a
# leg and one after and read any counter's delta afterwards without deciding in
# advance which counter mattered. Nothing for a non-etcfs backend.
compare_etcfs_snapshot_metrics() {
    [[ "$COMPARE_BACKEND_BASE" == "etcfs" ]] || return 0
    $SSH_CMD "ec2-user@$1" "curl -s --max-time 5 http://127.0.0.1:9090/metrics" \
        > "$RESULTS_DIR/metrics-$2.txt" 2>/dev/null || true
}

# compare_etcfs_page_cache <pub_ip> — whether the daemon is currently answering
# opens as cacheable: "up", "down", or "never" if it has had no client at all.
#
# Last line wins, because both messages mark a transition and what matters is
# the state they leave behind. Grepping for the startup "client connected" line
# alone is what let a mount report the cache as available while serving nothing
# from it: that line stays in the log after the connection dies.
compare_etcfs_page_cache() {
    [[ "$COMPARE_BACKEND_BASE" == "etcfs" ]] || return 0
    $SSH_CMD "ec2-user@$1" "sudo awk '
        /notify: client connected/ { s = \"up\" }
        /no cache-invalidation client is connected/ { s = \"down\" }
        END { print (s == \"\" ? \"never\" : s) }' /tmp/meta.log" 2>/dev/null
}

# compare_etcfs_cpu_profile <pub_ip> <label> <delay_s> <seconds> — take a CPU
# profile of the daemon while a workload is running, and leave it in the results
# directory. Backgrounded by the caller: the delay is what places the sampling
# window inside the run rather than at its start, where a warm-up is all there
# is to see. Needs --pprof on the daemon (ETCFS_META_EXTRA_ARGS).
compare_etcfs_cpu_profile() {
    [[ "$COMPARE_BACKEND_BASE" == "etcfs" ]] || return 0
    local ip="$1" label="$2" delay="$3" seconds="$4"
    sleep "$delay"
    $SSH_CMD "ec2-user@$ip" \
        "curl -s --max-time $((seconds + 60)) -o /tmp/cpu-$label.pprof \
             http://127.0.0.1:9090/debug/pprof/profile?seconds=$seconds" 2>/dev/null || return 0
    scp $SSH_OPTS "ec2-user@$ip:/tmp/cpu-$label.pprof" "$RESULTS_DIR/cpu-$label.pprof" >/dev/null 2>&1 || true
}

# compare_etcfs_notify_failures <pub_ip> — how many times a lock could not be
# yielded because the kernel's pages for it were not invalidated. Zero is the
# only acceptable number: each one is a lock this node keeps holding against the
# cluster, and a run full of them is the invalidation path failing under load
# rather than the filesystem being slow.
compare_etcfs_notify_failures() {
    [[ "$COMPARE_BACKEND_BASE" == "etcfs" ]] || return 0
    $SSH_CMD "ec2-user@$1" \
        "sudo grep -c 'kernel pages not invalidated' /tmp/meta.log || true" 2>/dev/null
}

# compare_drop_caches <pub_ip>... — empty the kernel's page, dentry and inode
# caches, so the next read is genuinely cold. Any measurement of a warm cache
# needs a cold one to be worth anything, and writing a file leaves it cached.
compare_drop_caches() {
    local ip
    for ip in "$@"; do
        $SSH_CMD "ec2-user@$ip" "sync; sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'"
    done
}

# compare_div <a> <b> [scale] — awk rather than bash arithmetic: every ratio
# here (MiB/s, ops/s, percentages) is fractional.
compare_div() {
    awk -v a="$1" -v b="$2" -v s="${3:-2}" 'BEGIN{ if (b+0 == 0) { print "0" } else { printf "%.*f", s, a/b } }'
}

# ---- fio with a caller-supplied job file ----
#
# run_fio (bench-lib.sh) hard-codes one randwrite+randread 4k shape, which is
# right for the IOPS comparison and wrong for every scenario here (sequential
# handoff, O_DSYNC, single-stream throughput). This keeps run_fio's ssh/copy/
# timeout handling and lets the caller own the job stanzas. Prints the local
# JSON path.
compare_run_job() {
    local label="$1" ip="$2" runtime="$3" job="$4"
    log "--- fio: $label on $ip ---"
    echo "$job" | $SSH_CMD "ec2-user@$ip" "cat > /tmp/${label}.fio"
    timeout "$((runtime * 4 + 120))" $SSH_CMD "ec2-user@$ip" \
        "sudo fio /tmp/${label}.fio --output-format=json --output=/tmp/${label}.json" >/dev/null
    scp $SSH_OPTS "ec2-user@$ip:/tmp/${label}.json" "$RESULTS_DIR/${label}.json" >/dev/null
    echo "$RESULTS_DIR/${label}.json"
}

# compare_parallel_fio <label> <runtime> <job_template> <pub_ip>... — one fio
# per node, all launched together and waited on afterwards, printing the summed
# write bandwidth in MiB/s. @NODE@ in the template is replaced by the node's
# index, which is how a caller asks for disjoint per-node files (leave it out
# and every node lands on the same path, which is the shared working set).
#
# run_fio_contention (bench-lib.sh) is the same idea for one shared 4k randrw
# shape; this one takes the caller's own job, because the scenarios that need
# it are measuring aggregate sequential bandwidth and load-while-something-else
# -happens, not contention on a single inode.
compare_parallel_fio() {
    local label="$1" runtime="$2" template="$3"
    shift 3
    local ips=("$@") i pids=() jsons=()
    for i in "${!ips[@]}"; do
        compare_run_job "$label-$i" "${ips[$i]}" "$runtime" "${template//@NODE@/$i}" >/dev/null &
        pids+=($!)
        jsons+=("$RESULTS_DIR/$label-$i.json")
    done
    local pid
    for pid in "${pids[@]}"; do wait "$pid" || die "compare_parallel_fio: a node failed during $label"; done
    jq -s 'map(.jobs[0].write.bw // 0) | (add // 0) / 1024 | . * 100 | round / 100' "${jsons[@]}"
}

# compare_fio_bw_mibps <json> <read|write> — fio reports bw in KiB/s.
compare_fio_bw_mibps() {
    jq -r --arg d "$2" '(.jobs[0][$d].bw // 0) / 1024 | . * 100 | round / 100' "$1"
}

compare_fio_iops() { jq -r --arg d "$2" '(.jobs[0][$d].iops // 0) | round' "$1"; }

# compare_parallel_fio <label> <runtime> <job> <ips...> — the same job on every
# named node at once, aggregated. Occurrences of @NODE@ in the job are replaced
# with the node's own IP, which is what makes a *disjoint* working set
# expressible (filename=.../w-@NODE@.dat) as well as a shared one (one literal
# path). Prints total write MiB/s across the nodes.
#
# run_fio_contention (bench-lib.sh) covers the shared-file case for the
# single-cluster suite; this one exists because the scaling curve needs both
# shapes and needs the aggregate as a number it can go on comparing across
# node counts.
compare_parallel_fio() {
    local label="$1" runtime="$2" job="$3"
    shift 3
    local ips=("$@") ip pids=()
    log "--- parallel fio: $label (${#ips[@]} nodes) ---"
    for ip in "${ips[@]}"; do
        echo "${job//@NODE@/$ip}" | $SSH_CMD "ec2-user@$ip" "cat > /tmp/${label}.fio"
        (
            timeout "$((runtime * 4 + 120))" $SSH_CMD "ec2-user@$ip" \
                "sudo fio /tmp/${label}.fio --output-format=json --output=/tmp/${label}.json" >/dev/null || true
            scp $SSH_OPTS "ec2-user@$ip:/tmp/${label}.json" "$RESULTS_DIR/${label}-$ip.json" >/dev/null
        ) &
        pids+=($!)
    done
    local failed=0
    for pid in "${pids[@]}"; do wait "$pid" || failed=$((failed + 1)); done
    [[ "$failed" -eq 0 ]] || log "WARNING: $failed/${#ips[@]} node(s) hit an error during $label"

    shopt -s nullglob
    local files=("$RESULTS_DIR/${label}"-*.json)
    shopt -u nullglob
    [[ "${#files[@]}" -gt 0 ]] || die "compare_parallel_fio: no results came back for $label"
    jq -s 'map(.jobs[0].write.bw // 0) | (add // 0) / 1024 | . * 100 | round / 100' "${files[@]}"
}

# ---- shared-directory metadata concurrency ----
#
# create + stat + unlink of <files_per_node> files by every node at once, all
# in one directory. The point is the shared directory: GFS2/OCFS2 bounce that
# directory's DLM lock per operation, so the number that matters is aggregate
# ops/sec against node count, taken over the slowest node (every node is
# contending for the whole of its own run, so the run isn't over until the
# last one finishes).
compare_metadata_ops() {
    local label="$1" dir="$2" per_node="$3"
    shift 3
    local ips=("$@")
    log "--- metadata concurrency: $label (${#ips[@]} nodes x $per_node files in $dir) ---"
    $SSH_CMD "ec2-user@${ips[0]}" "sudo mkdir -p $dir && sudo chmod 777 $dir"

    # A previous run of this label left one file per node it used, and the node
    # IPs differ between runs — so without this the glob below reads both runs
    # and reports whichever was slower. It is scoped to this label rather than
    # the whole results directory, which holds every other scenario's output.
    rm -f "$RESULTS_DIR/${label}"-*.elapsed

    # The trailing newline is load-bearing. These files are concatenated below,
    # and awk's %.3f without one glues every node's number into a single token:
    # "18.320" and "18.366" become "18.32018.366", which `sort -g` reads as one
    # value and awk then parses as 18.32018 — so the "slowest node" silently
    # became the first file in glob order with the rest as trailing digits.
    local remote="
        p=\$(hostname -s)
        s=\$(date +%s.%N)
        for i in \$(seq 1 $per_node); do : > $dir/\$p-\$i; done
        for i in \$(seq 1 $per_node); do stat $dir/\$p-\$i >/dev/null; done
        for i in \$(seq 1 $per_node); do rm -f $dir/\$p-\$i; done
        e=\$(date +%s.%N)
        awk -v s=\$s -v e=\$e 'BEGIN{printf \"%.3f\\n\", e-s}'
    "
    local ip pids=() i=0
    for ip in "${ips[@]}"; do
        $SSH_CMD "ec2-user@$ip" "$remote" > "$RESULTS_DIR/${label}-$ip.elapsed" &
        pids+=($!)
        i=$((i + 1))
    done
    local failed=0
    for pid in "${pids[@]}"; do wait "$pid" || failed=$((failed + 1)); done
    [[ "$failed" -eq 0 ]] || die "compare_metadata_ops: $failed/${#ips[@]} node(s) failed during $label"

    local slowest
    slowest=$(cat "$RESULTS_DIR/${label}"-*.elapsed | sort -g | tail -1)
    # 3 ops per file: create, stat, unlink.
    compare_div "$((${#ips[@]} * per_node * 3))" "$slowest"
}

# ---- small-file tree ----
#
# compare_build_tree <ip> <files> — a kernel-source-shaped tree (many small
# files, two directory levels) staged on the node's *local* disk as a tarball,
# so the untar below measures the filesystem under test and not the generator.
# Prints the tarball path. Skips the work if the tarball is already there,
# which is what lets one provisioned cluster run the storm and the walk.
COMPARE_TREE_TAR=/tmp/compare-tree.tar
compare_build_tree() {
    local ip="$1" count="${2:-80000}" per_dir=100
    $SSH_CMD "ec2-user@$ip" "
        set -e
        [[ -f $COMPARE_TREE_TAR ]] && exit 0
        rm -rf /tmp/compare-tree && mkdir -p /tmp/compare-tree
        cd /tmp/compare-tree
        for d in \$(seq 1 $((count / per_dir))); do
            mkdir -p d\$d
            for f in \$(seq 1 $per_dir); do
                head -c 2048 /dev/zero > d\$d/f\$f.c
            done
        done
        tar cf $COMPARE_TREE_TAR .
    "
    echo "$COMPARE_TREE_TAR"
}

# compare_untar_tree <ip> <dest_dir> — times an untar of that tarball onto the
# filesystem under test. Prints "<seconds> <files_per_second>".
compare_untar_tree() {
    local ip="$1" dest="$2" count="${3:-80000}"
    $SSH_CMD "ec2-user@$ip" "sudo rm -rf $dest && sudo mkdir -p $dest && sudo chmod 777 $dest"
    local elapsed
    elapsed=$(compare_remote_time "$ip" "sudo tar xf $COMPARE_TREE_TAR -C $dest && sync")
    echo "$elapsed $(compare_div "$count" "$elapsed")"
}

# compare_walk_tree <ip> <dir> — cold then warm `find` + `du` over a populated
# tree. Cold drops the client's caches first; the pair is the point, since the
# gap between them is the metadata-caching story. Prints
# "<cold_find_s> <warm_find_s> <du_s>".
compare_walk_tree() {
    local ip="$1" dir="$2"
    $SSH_CMD "ec2-user@$ip" "sudo sync; echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null"
    local cold warm du_s
    cold=$(compare_remote_time "$ip" "sudo find $dir -type f | wc -l")
    warm=$(compare_remote_time "$ip" "sudo find $dir -type f | wc -l")
    du_s=$(compare_remote_time "$ip" "sudo du -s $dir")
    echo "$cold $warm $du_s"
}

# ---- I/O probe: continuous writes with a per-attempt outcome log ----
#
# The recovery scenarios need to know when a survivor's I/O to a dead node's
# inodes *resumes*, which no single timed command can answer: the interesting
# interval starts at the kill and ends at an op that has not been issued yet.
# So the survivor writes in a loop the whole time, logging one line per
# attempt, and the number is extracted from that log afterwards against the
# kill's own timestamp.
# A tag names the probe, so a node can run more than one at a time: the
# takeover probe (writing a file the dead node held) has to run alongside the
# survivor's own, and they cannot share a log.
compare_probe_start() {
    local ip="$1" path="$2" tag="${3:-probe}"
    # timeout, because this touch is itself an operation on the filesystem under
    # test: starting a probe on a file whose lock holder has just died can block
    # for as long as the recovery being measured, and a synchronous ssh here
    # would hang the whole scenario rather than record it.
    timeout 20 $SSH_CMD "ec2-user@$ip" \
        "sudo rm -f /tmp/$tag.log /tmp/$tag.stop; sudo timeout 10 touch $path" >/dev/null 2>&1 || true
    $SSH_CMD -n -f "ec2-user@$ip" "sudo sh -c 'while [ ! -f /tmp/$tag.stop ]; do
        t=\$(date +%s.%N)
        if dd if=/dev/zero of=$path bs=4k count=1 conv=notrunc oflag=direct >/dev/null 2>&1; then
            echo \"\$t ok\"
        else
            echo \"\$t err\"
        fi
    done > /tmp/$tag.log' >/dev/null 2>&1"
}

compare_probe_stop() {
    local tag="${2:-probe}"
    $SSH_CMD "ec2-user@$1" "sudo touch /tmp/$tag.stop" || true
}

# ---- death watch: when the fault actually landed ----
#
# "Kill the victim" and "the victim is gone" are not the same instant, and the
# gap between them is not small: a sysrq power-off is issued in milliseconds but
# the machine was still answering 46 seconds later in one gluster run, because
# the kernel still has to get through its own shutdown. Timing recovery from the
# moment the kill was *issued* therefore charges every backend for its victim's
# shutdown as if it were recovery, and hides the real one inside it.
#
# So a survivor watches the victim's own port five times a second for the whole
# run and logs each attempt, and the death is read back out of that log
# afterwards — the same shape as the I/O probe, and for the same reason.
compare_death_watch_start() {
    local watcher="$1" target_priv="$2" port="$3"
    $SSH_CMD "ec2-user@$watcher" "rm -f /tmp/death.log /tmp/death.stop" || true
    $SSH_CMD -n -f "ec2-user@$watcher" "sh -c 'while [ ! -f /tmp/death.stop ]; do
        t=\$(date +%s.%N)
        if timeout 1 bash -c \"</dev/tcp/$target_priv/$port\" >/dev/null 2>&1; then
            echo \"\$t up\"
        else
            echo \"\$t down\"
        fi
        sleep 0.2
    done > /tmp/death.log' >/dev/null 2>&1"
}

compare_death_watch_stop() {
    $SSH_CMD "ec2-user@$1" "touch /tmp/death.stop" || true
}

# compare_death_watch_time <watcher> <since_epoch> — the epoch of the first
# failed connection at or after <since_epoch>, or -1 if the target never stopped
# answering. Prints the absolute epoch, not an interval: callers need it as the
# origin for the recovery measurement, not only as a duration.
compare_death_watch_time() {
    local watcher="$1" t0="$2"
    $SSH_CMD "ec2-user@$watcher" "cat /tmp/death.log" > "$RESULTS_DIR/death-$watcher.log" 2>/dev/null || true
    awk -v t0="$t0" '$2 == "down" && $1 >= t0 { printf "%.3f", $1; found=1; exit } END { if (!found) print "-1" }' \
        "$RESULTS_DIR/death-$watcher.log"
}

# compare_probe_silence <ip> <end_epoch> [tag] — seconds between the probe's
# last successful write and <end_epoch>.
#
# This is the number that tells a filesystem which never stalled apart from one
# that stopped answering altogether. Both look identical to
# compare_probe_recovery: its "longest gap between consecutive successes" needs
# a success on each side of the gap, so a probe that hangs and never writes
# again closes no gap and reports a small stall from before the failure. A
# client blocked forever on a dead server, or a lockspace stopped waiting for a
# fence that never comes, is exactly that case.
compare_probe_silence() {
    local ip="$1" end="$2" tag="${3:-probe}"
    awk -v e="$end" '$2 == "ok" { last = $1 } END {
        if (last == "") { print "-1" } else { printf "%.3f", e - last }
    }' "$RESULTS_DIR/$tag-$ip.log" 2>/dev/null || echo "-1"
}

# compare_probe_recovery <ip> <kill_epoch> — prints
# "<resume_s> <max_stall_s> <errors_after_kill>": time from the kill to the
# first write that succeeded after it, the longest gap between consecutive
# successes after it (the actual stall, if the first post-kill attempt happened
# to land before the failure did), and how many attempts failed outright.
compare_probe_recovery() {
    local ip="$1" t0="$2" tag="${3:-probe}"
    $SSH_CMD "ec2-user@$ip" "sudo cat /tmp/$tag.log" > "$RESULTS_DIR/$tag-$ip.log"
    awk -v t0="$t0" '
        $2 == "ok" {
            if (first == "" && $1 >= t0) first = $1
            if (prev != "" && prev >= t0 && $1 - prev > gap) gap = $1 - prev
            prev = $1
        }
        $2 == "err" && $1 >= t0 { errs++ }
        END {
            if (first == "") { print "-1 -1", errs+0; exit }
            printf "%.3f %.3f %d\n", first - t0, gap + 0, errs + 0
        }
    ' "$RESULTS_DIR/$tag-$ip.log"
}

source "$COMPARE_SCRIPT_DIR/compare-backends.sh"
