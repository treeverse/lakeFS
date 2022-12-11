package time

import (
	"time"

	"github.com/Shopify/go-lua"
)

const Iso8601Format = `2006-01-02T15:04:05-07:00`

func Open(l *lua.State) {
	timeOpen := func(l *lua.State) int {
		lua.NewLibrary(l, timeLibrary)
		return 1
	}
	lua.Require(l, "time", timeOpen, false)
	l.Pop(1)
}

var timeLibrary = []lua.RegistryFunction{
	{Name: "now", Function: now},
	{Name: "format", Function: format},
	{Name: "formatISO", Function: formatISO},
	{Name: "sleep", Function: sleep},
	{Name: "since", Function: since},
	{Name: "add", Function: add},
	{Name: "parse", Function: parse},
	{Name: "parseISO", Function: parseISO},
}

func format(l *lua.State) int {
	epochNanoToFormat := int64(lua.CheckNumber(l, 1))
	layout := lua.CheckString(l, 2)
	zone := lua.CheckString(l, 3)

	loc, err := time.LoadLocation(zone)
	if err != nil {
		lua.Errorf(l, err.Error())
	}

	unixTime := time.Unix(epochNanoToFormat/1e9, 0)
	timeInTimeZone := unixTime.In(loc)
	l.PushString(timeInTimeZone.Format(layout))

	return 1
}

func formatISO(l *lua.State) int {
	epochNanoToFormat := int64(lua.CheckNumber(l, 1))
	zone := lua.OptString(l, 2, "")

	loc, err := time.LoadLocation(zone)
	if err != nil {
		lua.Errorf(l, err.Error())
	}

	unixTime := time.Unix(epochNanoToFormat/1e9, 0)
	timeInTimeZone := unixTime.In(loc)
	l.PushString(timeInTimeZone.Format(Iso8601Format))

	return 1
}

func add(l *lua.State) int {
	start := int64(lua.CheckNumber(l, 1))
	startUnix := time.Unix(0, start)

	lua.CheckType(l, 2, lua.TypeTable)
	l.Field(2, "hour")
	l.Field(2, "minute")
	l.Field(2, "second")
	second := lua.OptInteger(l, -1, 0)
	minute := lua.OptInteger(l, -2, 0)
	hour := lua.OptInteger(l, -3, 0)
	l.Pop(3)

	inc := startUnix.Add(time.Hour*time.Duration(hour) + time.Minute*time.Duration(minute) + time.Second*time.Duration(second))
	l.PushNumber(float64(inc.UnixNano()))
	return 1
}

func sleep(l *lua.State) int {
	ns := lua.CheckInteger(l, 1)
	time.Sleep(time.Nanosecond * time.Duration(ns))
	return 1
}

func now(l *lua.State) int {
	l.PushNumber(float64(time.Now().UnixNano()))
	return 1
}

func since(l *lua.State) int {
	start := lua.CheckNumber(l, 1)
	diff := float64(time.Now().UnixNano()) - start
	l.PushNumber(diff)
	return 1
}

func parse(l *lua.State) int {
	format := lua.CheckString(l, 1)
	value := lua.CheckString(l, 2)

	return parseTime(l, format, value)
}

func parseISO(l *lua.State) int {
	value := lua.CheckString(l, 1)

	return parseTime(l, Iso8601Format, value)
}

func parseTime(l *lua.State, format, value string) int {
	t, err := time.Parse(format, value)
	if err != nil {
		lua.Errorf(l, err.Error())
	}

	l.PushNumber(float64(t.UnixNano()))

	return 1
}
