package serde

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
	Owner   Owner   `xml:"owner"`
}

type CreateBucketConfiguration struct {
	LocationConstraint string
}
