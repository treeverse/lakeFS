package block

import (
	"bytes"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"io/ioutil"
	"os"
	"strings"
	"testing"
)

const (
	EnvVarS3AccessKeyId = "AWS_ADAPTER_ACCESS_KEY_ID"
	EnvVarS3SecretKey   = "AWS_ADAPTER_SECRET_ACCESS_KEY"
	EnvVarS3Region      = "AWS_ADAPTER_REGION"
	EnvVarS3Token       = "AWS_ADAPTER_TOKEN"
	TestBucketName      = "guy-first"
)

func setUpS3Adapter() (Adapter, error) {
	sess := session.Must(session.NewSession(&aws.Config{
		Region:      aws.String(os.Getenv(EnvVarS3Region)),
		Credentials: credentials.NewStaticCredentials(os.Getenv(EnvVarS3AccessKeyId), os.Getenv(EnvVarS3SecretKey), os.Getenv(EnvVarS3Token))}))
	svc := s3.New(sess)
	return NewS3Adapter(svc)
}

func TestS3Adapter_Get(t *testing.T) {
	sf, err := setUpS3Adapter()
	if err != nil {
		t.Fatal(err)
	}
	fileName := "test_file"
	a := "small test"

	err = sf.Put(TestBucketName, fileName, bytes.NewReader([]byte(a)))
	if err != nil {
		t.Fatal(err)
	}

	reader, err := sf.Get(TestBucketName, fileName)
	if err != nil {
		t.Fatal(err)
	}
	if b, err := ioutil.ReadAll(reader); err == nil {
		if strings.Compare(a, string(b)) != 0 {
			t.Fatalf("wrote: %s and read: %s", a, string(b))
		}
	}
	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func TestS3Adapter_GetRange(t *testing.T) {
	sf, err := setUpS3Adapter()
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

	reader, err := sf.GetRange(TestBucketName, fileName, 1000, 2000)
	if err != nil {
		t.Fatal(err)
	}
	if b, err := ioutil.ReadAll(reader); err == nil {
		if strings.Compare(a[1000:2001], string(b)) != 0 {
			t.Fatalf("wrote: %s and read: %s", a[1000:2001], string(b))
		}
	}

	err = reader.Close()
	if err != nil {
		t.Fatal(err)
	}
}
