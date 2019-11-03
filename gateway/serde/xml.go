package serde

type Error struct {
	Code      string `xml:"Code"`
	Message   string `xml:"Message"`
	Region    string `xml:"Region"`
	RequestId string `xml:"RequestId"`
	HostId    string `xml:"HostId"`
}

type Owner struct {
	DisplayName string `xml:"DisplayName"`
	ID          string `xml:"ID"`
}

type Buckets struct {
	Bucket []Bucket `xml:"Bucket"`
}

type Bucket struct {
	CreationDate string `xml:"CreationDate"`
	Name         string `xml:"Name"`
}

type ListBucketsOutput struct {
	Buckets Buckets `xml:"Buckets"`
	Owner   Owner   `xml:"Owner"`
}

type CreateBucketConfiguration struct {
	LocationConstraint string `xml:"LocationConstraint"`
}

type Contents struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

type CommonPrefixes struct {
	Prefix string `xml:"Prefix"`
}

type ListObjectsV2Output struct {
	Name                  string           `xml:"Name"`
	IsTruncated           bool             `xml:"IsTruncated"`
	Prefix                string           `xml:"Prefix"`
	Delimiter             string           `xml:"Delimiter,omitempty"`
	KeyCount              int              `xml:"KeyCount"`
	MaxKeys               int              `xml:"MaxKeys"`
	CommonPrefixes        []CommonPrefixes `xml:"CommonPrefixes"`
	NextContinuationToken string           `xml:"NextContinuationToken,omitempty"`
	ContinuationToken     string           `xml:"ContinuationToken,omitempty"`
	Contents              []Contents       `xml:"Contents"`
}
