package myutils

func Insert[T any](array []T, i int, element T) []T {
	return append(array[:i], append([]T{element}, array[i:]...)...)
}
