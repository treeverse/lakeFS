package block

import (
	"bytes"
	"fmt"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"io"
	"io/ioutil"
	"strings"
	"testing"
)

const (
	TestBucketName = "test"
)

type localS3 map[string]string
type mockS3Client struct {
	lastRangeReceived  string
	lastKeyReceived    string
	lastBucketReceived string
	callCounter        int
	lastBodyReceived   io.ReadSeeker
	s3iface.S3API
	localS3
}

func newMock() *mockS3Client {
	return &mockS3Client{
		localS3: make(map[string]string),
	}
}

func (m *mockS3Client) PutObject(input *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
	m.callCounter++
	m.lastBucketReceived = *input.Bucket
	m.lastKeyReceived = *input.Key
	m.lastBodyReceived = input.Body
	return nil, nil
}

func (m *mockS3Client) GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	m.callCounter++
	m.lastBucketReceived = *input.Bucket
	m.lastKeyReceived = *input.Key
	if input.Range != nil {
		m.lastRangeReceived = *input.Range
	}
	return &s3.GetObjectOutput{Body: ioutil.NopCloser(bytes.NewReader([]byte("mock read data")))}, nil
}

func setUpMockS3Adapter() (*mockS3Client, Adapter, error) {
	mock := newMock()
	adapter, err := NewS3Adapter(mock)
	return mock, adapter, err
}

func TestS3Adapter_Put(t *testing.T) {
	mockObj, sf, err := setUpMockS3Adapter()
	if err != nil {
		t.Fatal(err)
	}
	fileName := "test_file"
	sendData := "small test"

	err = sf.Put(TestBucketName, fileName, bytes.NewReader([]byte(sendData)))
	if err != nil {
		t.Fatal(err)
	}

	// Test repo
	if strings.Compare(mockObj.lastBucketReceived, TestBucketName) != 0 {
		t.Fatalf("bucket should be equal to repo. bucket=%s, repo=%s", mockObj.lastBucketReceived, TestBucketName)
	}

	// Test key
	if strings.Compare(mockObj.lastKeyReceived, TestBucketName) != 0 {
		t.Fatalf("received unexpected key. expected=%s, received=%s", fileName, mockObj.lastKeyReceived)
	}

	// Test sent data
	receivedData, err := ioutil.ReadAll(mockObj.lastBodyReceived)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Compare(string(receivedData), sendData) != 0 {
		t.Fatalf("received unexpected key. expected=%s, received=%s", fileName, mockObj.lastKeyReceived)
	}
}

func TestS3Adapter_GetRange(t *testing.T) {
	mockObj, sf, err := setUpMockS3Adapter()
	if err != nil {
		t.Fatal(err)
	}
	fileName := "test_file"
	a := `Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Facilisis leo vel fringilla est ullamcorper eget. Vitae elementum curabitur vitae nunc sed velit dignissim sodales. Eu ultrices vitae auctor eu. Eleifend donec pretium vulputate sapien nec sagittis aliquam. Diam vel quam elementum pulvinar etiam non. Nisl nunc mi ipsum faucibus vitae aliquet nec ullamcorper. Feugiat sed lectus vestibulum mattis ullamcorper velit sed. Quis commodo odio aenean sed adipiscing. Rhoncus urna neque viverra justo nec. Convallis posuere morbi leo urna molestie at elementum. Eros in cursus turpis massa. Ultrices gravida dictum fusce ut placerat.

	Lectus urna duis convallis convallis tellus. Mauris rhoncus aenean vel elit scelerisque. Tortor posuere ac ut consequat semper. Fermentum dui faucibus in ornare quam viverra orci sagittis eu. Feugiat vivamus at augue eget arcu dictum varius duis. Nec feugiat in fermentum posuere urna. Nibh venenatis cras sed felis eget. Semper feugiat nibh sed pulvinar proin gravida. Aliquet nibh praesent tristique magna sit amet purus. Donec enim diam vulputate ut pharetra. Dignissim cras tincidunt lobortis feugiat vivamus. Amet nisl suscipit adipiscing bibendum. Diam volutpat commodo sed egestas egestas fringilla phasellus. Penatibus et magnis dis parturient. Dignissim cras tincidunt lobortis feugiat vivamus. Libero volutpat sed cras ornare arcu dui vivamus arcu.

		Massa vitae tortor condimentum lacinia quis vel eros donec. Ut sem nulla pharetra diam sit amet. Commodo viverra maecenas accumsan lacus vel facilisis volutpat est. Amet aliquam id diam maecenas ultricies mi. Neque viverra justo nec ultrices dui sapien eget mi proin. Enim ut tellus elementum sagittis vitae et leo duis ut. Lacus luctus accumsan tortor posuere. Condimentum vitae sapien pellentesque habitant. Habitant morbi tristique senectus et netus. Porttitor leo a diam sollicitudin tempor id. Ac turpis egestas sed tempus urna et pharetra pharetra massa. Amet nulla facilisi morbi tempus. Ultrices eros in cursus turpis massa tincidunt. Aliquam purus sit amet luctus venenatis lectus. Nunc scelerisque viverra mauris in aliquam. Augue ut lectus arcu bibendum at varius.

		Tellus at urna condimentum mattis pellentesque id nibh tortor. Pellentesque id nibh tortor id aliquet. Lacus viverra vitae congue eu consequat ac felis donec. Vel facilisis volutpat est velit egestas dui. Turpis egestas sed tempus urna et pharetra pharetra massa massa. Sed felis eget velit aliquet sagittis id consectetur purus ut. A arcu cursus vitae congue mauris rhoncus aenean vel elit. Vel quam elementum pulvinar etiam non quam lacus. Adipiscing elit duis tristique sollicitudin nibh sit amet. Sagittis nisl rhoncus mattis rhoncus urna neque. Adipiscing elit pellentesque habitant morbi tristique senectus. Egestas dui id ornare arcu odio ut sem nulla pharetra. In cursus turpis massa tincidunt dui ut. Metus vulputate eu scelerisque felis imperdiet proin. Et ligula ullamcorper malesuada proin libero nunc. Risus nec feugiat in fermentum posuere urna nec tincidunt praesent. Consectetur purus ut faucibus pulvinar.

		Viverra maecenas accumsan lacus vel facilisis. In mollis nunc sed id semper risus. Cursus sit amet dictum sit amet justo donec enim diam. In arcu cursus euismod quis viverra. Vestibulum lectus mauris ultrices eros in cursus turpis massa. Odio ut enim blandit volutpat maecenas volutpat blandit aliquam. Laoreet suspendisse interdum consectetur libero id faucibus nisl tincidunt eget. Tellus in metus vulputate eu scelerisque felis imperdiet proin fermentum. Nulla pellentesque dignissim enim sit amet venenatis. Faucibus vitae aliquet nec ullamcorper sit. Leo vel orci porta non pulvinar neque laoreet. Leo urna molestie at elementum eu facilisis sed. Aliquam ut porttitor leo a diam sollicitudin tempor id.`

	err = sf.Put(TestBucketName, fileName, bytes.NewReader([]byte(a)))
	if err != nil {
		t.Fatal(err)
	}
	rangeStart := int64(1000)
	rangeEnd := int64(2000)
	_, err = sf.GetRange(TestBucketName, fileName, rangeStart, rangeEnd)
	if err != nil {
		t.Fatal(err)
	}

	//test repo
	if strings.Compare(mockObj.lastBucketReceived, TestBucketName) != 0 {
		t.Fatalf("bucket should be equal to repo. bucket=%s, repo=%s", mockObj.lastBucketReceived, TestBucketName)
	}

	//test key
	if strings.Compare(mockObj.lastKeyReceived, fileName) != 0 {
		t.Fatalf("received unexpected key. expected=%s, received=%s", fileName, mockObj.lastKeyReceived)
	}

	//test range
	expectedRange := fmt.Sprintf("bytes=%d-%d", rangeStart, rangeEnd)
	if strings.Compare(expectedRange, mockObj.lastRangeReceived) != 0 {
		t.Fatalf("recieved unexpected range. expected:'%s' , received:'%s' ", expectedRange, mockObj.lastRangeReceived)
	}
}

func TestMultipleReads(t *testing.T) {
	mockData, sf, err := setUpMockS3Adapter()
	if err != nil {
		t.Fatal(err)
	}
	mockData.callCounter = 0
	fileName := "test_file"
	reader, err := sf.Get(TestBucketName, fileName)
	if err != nil {
		t.Fatal(err)
	}
	buffSize := 2
	buff := make([]byte, buffSize)
	for i := 1; i < 4; i++ {
		_, err = reader.Read(buff)

	}

	if mockData.callCounter != 1 {
		t.Fatalf("expected get to be called only once (regardless the numnber of reads)")
	}

}
