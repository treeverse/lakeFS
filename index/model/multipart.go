package model

import (
	"time"
)

type MultipartUpload struct {
	RepositoryId string    `db:"repository_id"`
	Id           string    `db:"id"`
	Path         string    `db:"path"`
	CreationDate time.Time `db:"creation_date"`
	ObjectName   string    `db:"object_name"`
}

type MultipartUploadPart struct {
	RepositoryId string    `db:"repository_id"`
	UploadId     string    `db:"upload_id"`
	PartNumber   int       `db:"part_number"`
	Checksum     string    `db:"checksum"`
	CreationDate time.Time `db:"creation_date"`
	Size         int64     `db:"size"`
}

//func (m *MultipartUploadPart) Identity() []byte {
//	addresses := make([]string, len(m.Blocks))
//	for i, block := range m.Blocks {
//		addresses[i] = block.Address
//	}
//	return identFromStrings(addresses...)
//}

type MultipartUploadPartRequest struct {
	PartNumber int32
	Etag       string
}
