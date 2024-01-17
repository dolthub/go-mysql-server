package stats

func floatMin(i, j float64) float64 {
	if i < j {
		return i
	}
	return j
}

func floatMax(i, j float64) float64 {
	if i > j {
		return i
	}
	return j
}
