variable "cluster_name" {
  description = "Cluster tag/name prefix. Every resource is tagged ClusterName=<this>; instances additionally get Role=etcfs-node, which is what the user-data peer-discovery query filters on."
  type        = string
  default     = "etcfuse"
}

variable "az" {
  description = "Availability zone. The ASG, its instances and the shared EBS volume must be in the same AZ or the attach fails."
  type        = string
  default     = "eu-west-1a"
}

variable "min_size" {
  description = "Minimum ASG size. 3 minimum for etcd quorum to survive one node loss."
  type        = number
  default     = 3

  validation {
    condition     = var.min_size >= 1
    error_message = "min_size must be at least 1."
  }
}

variable "max_size" {
  type    = number
  default = 5
}

variable "desired_capacity" {
  type    = number
  default = 3
}

variable "instance_type" {
  type    = string
  default = "t3.medium"
}

variable "volume_size_gb" {
  description = "Size of the shared raw EBS volume. EtcFS uses it as a raw block device — it is never formatted."
  type        = number
  default     = 10
}

variable "volume_iops" {
  type    = number
  default = 100
}

variable "root_volume_size_gb" {
  type    = number
  default = 20
}

variable "key_name" {
  description = "Name of the EC2 key pair to create from public_key_path. Empty derives '<cluster_name>-key'."
  type        = string
  default     = ""
}

variable "public_key_path" {
  type    = string
  default = "~/.ssh/id_ed25519.pub"
}

variable "subnet_id" {
  description = "Subnet to launch into. Empty resolves the default VPC's subnet in var.az."
  type        = string
  default     = ""
}

variable "ssh_ingress_cidr" {
  description = "CIDR allowed to SSH in. Empty auto-detects the caller's public IP as a /32."
  type        = string
  default     = ""
}

variable "instance_profile" {
  description = "Pre-existing IAM instance profile the nodes run under, created once per account by scripts/infra/fencing-iam.sh. Must include AmazonSSMManagedInstanceCore for the graceful scale-in Lambda to reach nodes via SSM Run Command."
  type        = string
  default     = "etcfs-nodes"
}

variable "etcfs_version" {
  description = "EtcFS release tag to install on boot (e.g. \"0.35.0\"). Empty resolves the latest GitHub release."
  type        = string
  default     = ""
}

variable "etcd_version" {
  type    = string
  default = "v3.5.18"
}

variable "lease_ttl" {
  type    = string
  default = "10s"
}

variable "github_repo" {
  description = "owner/repo the release assets and version lookup are pulled from."
  type        = string
  default     = "etcfs/etcfs"
}
