package multiparts

import (
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"testing"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
)

var testData = MultipartUpload{
	UploadID:        "ExampleID",
	Path:            "ExamplePath",
	CreationDate:    time.Time{},
	PhysicalAddress: "SomePhysicalAddress",
	Metadata:        map[string]string{"key1": "value1", "key2": "value2"},
	ContentType:     "SomeContentType",
}

func serializeDataFlatBuff(b *testing.B) []byte {
	//b.Log("Serialize data")
	builder := flatbuffers.NewBuilder(0)

	// Create Metadata vector
	var vec []flatbuffers.UOffsetT
	for k, v := range testData.Metadata {
		key := builder.CreateString(k)
		value := builder.CreateString(v)
		MetadataFBStart(builder)
		MetadataFBAddKey(builder, key)
		MetadataFBAddValue(builder, value)
		vec = append(vec, MetadataFBEnd(builder))
	}
	// Populate Vector
	MultipartUploadFBStartMetadataVector(builder, len(testData.Metadata))
	for _, v := range vec {
		builder.PrependUOffsetT(v)
	}
	metadata := builder.EndVector(len(testData.Metadata))

	uploadID := builder.CreateString(testData.UploadID)
	path := builder.CreateString(testData.Path)
	t := uint64(testData.CreationDate.Unix())
	address := builder.CreateString(testData.PhysicalAddress)
	content := builder.CreateString(testData.ContentType)

	MultipartUploadFBStart(builder)
	MultipartUploadFBAddUploadId(builder, uploadID)
	MultipartUploadFBAddPath(builder, path)
	MultipartUploadFBAddCreationDate(builder, t)
	MultipartUploadFBAddPhysicalAddress(builder, address)
	MultipartUploadFBAddMetadata(builder, metadata)
	MultipartUploadFBAddContentType(builder, content)
	mp := MultipartUploadFBEnd(builder)
	builder.Finish(mp)

	buf := builder.FinishedBytes()
	//b.Log("Buffer size: ", len(buf)) // 224
	return buf
}

func serializeDataProtoBuff(b *testing.B) []byte {
	//b.Log("Serialize data")
	buf, _ := proto.Marshal(&MultipartUploadPB{
		UploadId:        testData.UploadID,
		Path:            testData.Path,
		CreationDate:    timestamppb.New(testData.CreationDate),
		PhysicalAddress: testData.PhysicalAddress,
		Metadata:        testData.Metadata,
		ContentType:     testData.ContentType,
	})
	//b.Log("Buffer size: ", len(buf)) // 107
	return buf
}

func BenchmarkFlatBuffer(b *testing.B) {
	buf := serializeDataFlatBuff(b)

	mtUp := GetRootAsMultipartUploadFB(buf, 0)
	_ = mtUp.UploadId()
	_ = mtUp.Path()
	_ = mtUp.CreationDate()
	_ = mtUp.PhysicalAddress()
	// Access table vector
	metadataLen := mtUp.MetadataLength()
	for i := 0; i < metadataLen; i++ {
		metadata := new(MetadataFB)
		mtUp.Metadata(metadata, 0)
		_ = metadata.Key()
		_ = metadata.Value()
	}

	_ = mtUp.ContentType()
}

func BenchmarkProtoBuffer(b *testing.B) {
	buf := serializeDataProtoBuff(b)

	// Unmarshal buffer
	mtUp := &MultipartUploadPB{}
	proto.Unmarshal(buf, mtUp)
	_ = mtUp.UploadId
	_ = mtUp.Path
	_ = mtUp.CreationDate
	_ = mtUp.PhysicalAddress
	for range mtUp.Metadata {
	}
	_ = mtUp.ContentType
}
