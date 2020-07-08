package glueClient

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"

	"github.com/aws/aws-sdk-go/service/glue"
	"github.com/aws/aws-sdk-go/service/glue/glueiface"
)

func TestGlueMSClient_GetTable(t *testing.T) {
	type fields struct {
		Svc       glueiface.GlueAPI
		CatalogID *string
	}
	type args struct {
		dbName  string
		tblName string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantR   *glue.TableData
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := GlueMSClient{
				svc:       tt.fields.Svc,
				catalogID: tt.fields.CatalogID,
			}
			gotR, err := g.GetTable(tt.args.dbName, tt.args.tblName)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetTable() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotR, tt.wantR) {
				t.Errorf("GetTable() gotR = %v, want %v", gotR, tt.wantR)
			}
		})
	}
}
func getService() *glue.Glue {
	cfg := &aws.Config{
		Region: aws.String("us-east-1"),
		//Logger: &config.LogrusAWSAdapter{},
	}

	cfg.Credentials = credentials.NewStaticCredentials(
		"AKIA6HHRMQLJMDVXY6OR",
		"iTqXRFn9Z2Vo04zD8lPAa+rJovheYi4eDt6hAtmN",
		"")

	sess := session.Must(session.NewSession(cfg))
	sess.ClientConfig("glue")

	return glue.New(sess)
}
func TestGlueMSClient_AddPartitions(t *testing.T) {
	type fields struct {
		Svc       glueiface.GlueAPI
		CatalogID *string
	}
	type args struct {
		dbName     string
		tableName  string
		partitions []*glue.Partition
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{
			name: "add partitions",
			fields: fields{
				Svc:       getService(),
				CatalogID: aws.String("977611293394"),
			},
			args: args{
				dbName:    "default",
				tableName: "imdb_nice_date_2",
				partitions: []*glue.Partition{
					{
						//CreationTime:      nil,
						DatabaseName: aws.String("default"),
						//LastAccessTime:    nil,
						//LastAnalyzedTime:  nil,
						//Parameters:        nil,
						StorageDescriptor: &glue.StorageDescriptor{
							Location: aws.String("s3a://example/br1/collection/shows/titles_by_year/startYear=2017"),
							SerdeInfo: &glue.SerDeInfo{
								Name: aws.String("imdb_nice_date_2"),
							},
						},
						TableName: aws.String("imdb_nice_date_2"),
						Values:    []*string{aws.String("2017")},
					},
					{
						//CreationTime:      nil,
						DatabaseName: aws.String("default"),
						//LastAccessTime:    nil,
						//LastAnalyzedTime:  nil,
						//Parameters:        nil,
						StorageDescriptor: &glue.StorageDescriptor{
							Location: aws.String("s3a://example/br1/collection/shows/titles_by_year/startYear=2018"),
							SerdeInfo: &glue.SerDeInfo{
								Name: aws.String("imdb_nice_date_2"),
							},
						},
						TableName: aws.String("imdb_nice_date_2"),
						Values:    []*string{aws.String("2018")},
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GlueMSClient{
				svc:       tt.fields.Svc,
				catalogID: tt.fields.CatalogID,
			}
			if err := g.addPartitions(tt.args.dbName, tt.args.tableName, tt.args.partitions); (err != nil) != tt.wantErr {
				t.Errorf("addPartitions() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
func TestAlterPartition(t *testing.T) {
	g := &GlueMSClient{
		svc:       getService(),
		catalogID: aws.String("977611293394"),
	}

	partitions, err := g.getAllPartitions("default", "imdb_nice_date_2")
	if err != nil {
		t.Error(err)
	}
	partition := partitions[1]
	fmt.Printf("working on partition %s", aws.StringValue(partition.Values[0]))
	mp := make(map[string]*string)
	mp["what"] = aws.String("OK")
	partition.Parameters = mp
	np := []*glue.Partition{partition}
	err = g.alterPartitions("default", "imdb_nice_date_2", np)
	if err != nil {
		fmt.Printf(err.Error())
	}
	fmt.Print("done")
}
