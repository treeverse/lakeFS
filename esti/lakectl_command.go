package esti

import "strings"

type LakeCtlCmd struct {
	rawCmd string
}

func NewLakeCtl() *LakeCtlCmd {
	return &LakeCtlCmd{
		rawCmd: Lakectl(),
	}
}

func (l *LakeCtlCmd) Arg(arg string) *LakeCtlCmd {
	l.rawCmd += " " + arg
	return l
}

// Flag Same as Arg, added for usage clarity.
func (l *LakeCtlCmd) Flag(arg string) *LakeCtlCmd {
	return l.Arg(arg)
}

func (l *LakeCtlCmd) PathArg(components ...string) *LakeCtlCmd {
	l.rawCmd += " " + strings.Join(components, "/")
	return l
}

func (l *LakeCtlCmd) URLArg(schemaPrefix string, components ...string) *LakeCtlCmd {
	l.rawCmd += " " + schemaPrefix + strings.Join(components, "/")
	return l
}

func (l *LakeCtlCmd) Get() string {
	return l.rawCmd
}
