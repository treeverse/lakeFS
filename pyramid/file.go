package pyramid

import "os"

// File is pyramid wrapper for os.file that triggers pyramid hooks for file actions.
type File struct {
	fh     *os.File
	access *accessManager

	filename string

	close func(size int64) error
	size  int64
}

func (f *File) Read(p []byte) (n int, err error) {
	f.access.touch(f.filename)
	return f.fh.Read(p)
}

func (f *File) ReadAt(p []byte, off int64) (n int, err error) {
	f.access.touch(f.filename)
	return f.fh.ReadAt(p, off)
}

func (f *File) Write(p []byte) (n int, err error) {
	f.access.touch(f.filename)
	s, err := f.fh.Write(p)
	f.size += int64(s)
	return s, err
}

func (f *File) Stat() (os.FileInfo, error) {
	return f.fh.Stat()
}

func (f *File) Sync() error {
	return f.fh.Sync()
}

func (f *File) Close() error {
	f.access.removeRef(f.filename)
	if f.close != nil {
		if err := f.close(f.size); err != nil {
			return err
		}
	}

	return f.fh.Close()
}
