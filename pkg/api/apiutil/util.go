package apiutil

func IsStatusCodeOK(statusCode int) bool {
	return statusCode >= 200 && statusCode <= 299
}

func Value[T any](ptr *T) T {
	if ptr == nil {
		return *new(T)
	}
	return *ptr
}

func MapValue[K comparable, V any](m *map[K]V) map[K]V {
	if m == nil {
		return make(map[K]V)
	}
	return *m
}

func Ptr[T any](val T) *T {
	return &val
}
