package utils

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/manifoldco/promptui"
	"github.com/spf13/pflag"
)

const (
	AutoConfirmFlagName     = "yes"
	AutoConfigFlagShortName = "y"
	AutoConfirmFlagHelp     = "Automatically say yes to all confirmations"

	StdinFileName = "-"
)

func AssignAutoConfirmFlag(flags *pflag.FlagSet) {
	flags.BoolP(AutoConfirmFlagName, AutoConfigFlagShortName, false, AutoConfirmFlagHelp)
}

func Confirm(flags *pflag.FlagSet, question string) (bool, error) {
	yes, err := flags.GetBool(AutoConfirmFlagName)
	if err == nil && yes {
		// got auto confirm flag
		return true, nil
	}
	prm := promptui.Prompt{
		Label:     question,
		IsConfirm: true,
	}
	_, err = prm.Run()
	if err != nil {
		return false, err
	}
	return true, nil
}

// nopCloser wraps a ReadSeekCloser to ignore calls to Close().  It is io.NopCloser (or
// ioutils.NopCloser) for Seeks.
type nopCloser struct {
	io.ReadSeekCloser
}

func (nc *nopCloser) Close() error {
	return nil
}

// deleteOnClose wraps a File to be a ReadSeekCloser that deletes itself when closed.
type deleteOnClose struct {
	*os.File
}

func (d *deleteOnClose) Close() error {
	if err := os.Remove(d.Name()); err != nil {
		_ = d.File.Close() // "Only" file descriptor leak if close fails (but data might stay).
		return fmt.Errorf("delete on close: %w", err)
	}
	return d.File.Close()
}

// OpenByPath returns a reader from the given path. If path is "-", it consumes Stdin and
// opens a readable copy that is either deleted (POSIX) or will delete itself on close
// (non-POSIX, notably WINs).
func OpenByPath(path string) io.ReadSeekCloser {
	if path == StdinFileName {
		if !IsSeekable(os.Stdin) {
			temp, err := os.CreateTemp("", "lakectl-stdin")
			if err != nil {
				DieErr(fmt.Errorf("create temporary file to buffer stdin: %w", err))
			}
			if _, err = io.Copy(temp, os.Stdin); err != nil {
				DieErr(fmt.Errorf("copy stdin to temporary file: %w", err))
			}
			if _, err = temp.Seek(0, io.SeekStart); err != nil {
				DieErr(fmt.Errorf("rewind temporary copied file: %w", err))
			}
			// Try to delete the file.  This will fail on Windows, we shall try to
			// delete on close anyway.
			if os.Remove(temp.Name()) != nil {
				return &deleteOnClose{temp}
			}
			return temp
		}
		return &nopCloser{os.Stdin}
	}
	fp, err := os.Open(path)
	if err != nil {
		DieErr(err)
	}
	return fp
}

func MustString(v string, err error) string {
	if err != nil {
		DieErr(err)
	}
	return v
}

func MustStringSlice(v []string, err error) []string {
	if err != nil {
		DieErr(err)
	}
	return v
}

func MustSliceNonEmptyString(list string, v []string) []string {
	for _, s := range v {
		if s == "" {
			DieErr(fmt.Errorf("error in %s list: %w", list, ErrInvalidValueInList))
		}
	}
	return v
}

func MustInt(v int, err error) int {
	if err != nil {
		DieErr(err)
	}
	return v
}

func MustInt64(v int64, err error) int64 {
	if err != nil {
		DieErr(err)
	}
	return v
}

func MustBool(v bool, err error) bool {
	if err != nil {
		DieErr(err)
	}
	return v
}

func MustDuration(v time.Duration, err error) time.Duration {
	if err != nil {
		DieErr(err)
	}
	return v
}

// IsSeekable returns true if f.Seek appears to work.
func IsSeekable(f io.Seeker) bool {
	_, err := f.Seek(0, io.SeekCurrent)
	return err == nil // a little naive, but probably good enough for its purpose
}
