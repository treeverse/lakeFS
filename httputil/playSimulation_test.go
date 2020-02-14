package httputil

import (
	"github.com/gorilla/mux"
	"testing"
)

func TestDoTestRun(t *testing.T) {
	router := mux.NewRouter()
	DoTestRun(*router)
}

//func TestRead( t*testing.T){
//	var base [1000]byte
//	cc := NewReader(129)
//	b := base[0:15]
//	l,err := cc.Read(b)
//	fmt.Print(l,err)
//	fmt.Print(string(b))
//	b = base[15:30]
//	l,err = cc.Read(b)
//	fmt.Print(string(b))
//	b = base[30:40]
//	l,err = cc.Read(b)
//	fmt.Print(string(b))
//	b = base[40:75]
//	l,err = cc.Read(b)
//	b = base[75:200]
//	l,err = cc.Read(b)
//	fmt.Print(string(b))
//cc.pos = 0
//z := base[900:901]
//copy(base[0:150],bytes.Repeat(z,150))
//for i:= 0;i < 5;i++{
//	b = base[i:i+1]
//	l,err := cc.Read(b)
//	fmt.Print(l,err)
//
//}
//fmt.Print(b)
//for i:= 5;i < cc.maxLength + 5;i++{
//	b = base[i:i+1]
//	l,err := cc.Read(b)
//	fmt.Print(l,err)

//}
//fmt.Print(l,err)

//}

//func TestSeek(t *testing.T){
//	var base [1000]byte
//	b := base[:0]
//	cc := NewReader(129)
//	err := cc.Seek(8)
//	if err != nil {
//		t.Error("could not seek to 8, got error ",err)
//	} else {
//		b = base[:2]
//		_,err = cc.Read(b)
//		if string(b) != "10"{
//			t.Error("wanted '10' got ",string(b))
//		}
//	}
//	err = cc.Seek(0)
//	if err != nil {
//		t.Error("could not seek to 0, got error ",err)
//	}
//	err = cc.Seek(300)
//	if err != io.EOF {
//		t.Error(" seek to 300, wanted EOF, got error ",err)
//	}
//	err = cc.Close()
//	err = cc.Seek(40)
//	t.Error(" seek after close, wanted panic, got error ",err)
//}
