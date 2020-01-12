package main
import (
	"fmt"
	"os"
)

func main() {
	//f := isDirectoryWritable("C:\\Program Files")
	f := isDirectoryWritable("")
	fmt.Println(f)
}

func isDirectoryWritable(path string)  bool {
	// test ability to write to directory.
	// as there is no simple way to test this in windows, I prefer the "brute force" method
	// of creating s dummy file. will work in any OS.
	// speed is not an issue, as this will be activated very few times during startup

	fileName := path + "dummy.tmp"
	os.Remove(fileName)
	file,err := os.Create(fileName)
	if err == nil {
		file.Close()
		os.Remove(fileName)
		return true
	} else {
		return false}
}

