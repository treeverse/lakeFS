package main

import (
	//"fmt"
	"log"
	"sort"
	"time"

	"github.com/bxcodec/faker/v3"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

type user struct {
	ID        string    `parquet:"name=id, type=UTF8, encoding=PLAIN_DICTIONARY"`
	FirstName string    `parquet:"name=firstname, type=UTF8, encoding=PLAIN_DICTIONARY"`
	LastName  string    `parquet:"name=lastname, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Email     string    `parquet:"name=email, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Phone     string    `parquet:"name=phone, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Blog      string    `parquet:"name=blog, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Username  string    `parquet:"name=username, type=UTF8, encoding=PLAIN_DICTIONARY"`
	Score     float64   `parquet:"name=score, type=DOUBLE"`
	CreatedAt time.Time //wont be saved in the parquet file
}

const recordNumber = 1000

func main() {
	var data []*user
	//create fake data
	for i := 0; i < recordNumber; i++ {
		u := &user{
			ID:        faker.UUIDDigit(),
			FirstName: faker.FirstName(),
			LastName:  faker.LastName(),
			Email:     faker.Email(),
			Phone:     faker.Phonenumber(),
			Blog:      faker.URL(),
			Username:  faker.Username(),
			Score:     float64(i),
			CreatedAt: time.Now(),
		}
		data = append(data, u)
	}
	sort.Slice(data, func(i, j int) bool { return data[i].FirstName < data[j].FirstName })
	err := generateParquet(data)
	if err != nil {
		log.Fatal(err)
	}
}

func generateParquet(data []*user) error {
	log.Println("generating parquet file")
	fw, err := local.NewLocalFileWriter("output.parquet")
	if err != nil {
		return err
	}
	//parameters: writer, type of struct, size
	pw, err := writer.NewParquetWriter(fw, new(user), int64(len(data)))
	if err != nil {
		return err
	}
	//compression type
	pw.CompressionType = parquet.CompressionCodec_UNCOMPRESSED
	defer fw.Close()
	for _, d := range data {
		if err = pw.Write(d); err != nil {
			return err
		}
	}
	if err = pw.WriteStop(); err != nil {
		return err
	}
	return nil
}
