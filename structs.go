package main

type Train struct {
	id   int
	name string
}

type Station struct {
	id   int
	name string
	code int
}

type Route struct {
	id     int
	dep_id int
	arr_id int
}

type TimeTable struct {
	id          int
	route_id    int
	dep         float64
	arr         float64
	time_travel float64
}
