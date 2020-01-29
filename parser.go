package main

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/opesun/goquery"
)

var (
	WORKERS int      = 5000 // кол-во "потоков"
	urls    []string = []string{}
	wg      sync.WaitGroup
	db      *sql.DB
	counter int64
	mu      sync.Mutex
)

func Request(url string) string {
	resp, err := http.Get(url)
	if err != nil {
		log.Fatalln(err)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatalln(err)
	}

	return string(body)
}

func select_train(name string) (Train, bool) {
	t := Train{}
	mu.Lock()
	rows, err := db.Query("SELECT * FROM train WHERE name=$1;", name)
	defer mu.Unlock()
	defer rows.Close()
	if err != nil {
		panic(err)
	}
	if !rows.Next() {
		return t, false
	}
	rows.Scan(&t.id, &t.name)
	return t, true
}

func select_station(name string) (Station, bool) {
	st := Station{}
	mu.Lock()
	rows, err := db.Query("SELECT * FROM station WHERE name=$1;", name)
	defer mu.Unlock()
	defer rows.Close()
	if err != nil {
		panic(err)
	}
	if !rows.Next() {
		return st, false
	}
	rows.Scan(&st.id, &st.name, &st.code)
	return st, true
}

func select_route(dep_id string, arr_id string) (Route, bool) {
	r := Route{}
	mu.Lock()
	rows, err := db.Query("SELECT * FROM route WHERE dep_id=$1 AND arr_id=$2", dep_id, arr_id)
	defer mu.Unlock()
	defer rows.Close()
	if err != nil {
		panic(err)
	}
	if !rows.Next() {
		return r, false
	}
	rows.Scan(r.id, r.dep_id, r.arr_id)
	return r, true
}

func select_timetable(route_id int64, dep_time int64, arr_time int64, time_travel int64) (TimeTable, bool) {
	tt := TimeTable{}
	mu.Lock()
	rows, err := db.Query("SELECT * FROM time_table WHERE route_id=$1 AND dep_time=$2 AND arr_time=$3 AND time_travel=$4", route_id, dep_time, arr_time, time_travel)
	defer mu.Unlock()
	defer rows.Close()
	if err != nil {
		panic(err)
	}
	if !rows.Next() {
		return tt, false
	}
	rows.Scan(&tt.route_id, &tt.dep, &tt.arr, &tt.time_travel)
	return tt, true
}

func insert_route(dep_id string, arr_id string) (sql.Result, error) {
	d, err := strconv.ParseInt(dep_id, 10, 64)
	if err != nil {
		panic(err)
	}
	a, err := strconv.ParseInt(arr_id, 10, 64)
	return db.Exec("INSERT INTO route(dep_id, arr_id) VALUES ($1, $2)", d, a)
}

func insert_train(name string) (sql.Result, error) {
	return db.Exec("INSERT INTO train(name) VALUES ($1)", name)
}

func insert_timetable(route_id int64, dep_time int64, arr_time int64, time_travel int64) (sql.Result, error) {
	return db.Exec("INSERT INTO time_table(route_id, dep_time, arr_time, time_travel) VALUES ($1, $2, $3, $4)", route_id, dep_time, arr_time, time_travel)
}

func insert_price(time_table_id int64, typ string, price_precise string, price_tenge int) (sql.Result, error) {
	return db.Exec("INSERT INTO price(time_table_id, type, price_precise, price_tenge) VALUES ($1, $2, $3, $4)", time_table_id, typ, price_precise, price_tenge)
}

func Split(s string, cutset string) []string {
	return strings.Split(s, cutset)
}

func Join(s []string, sep string) string {
	return strings.Join(s, sep)
}

type Train struct {
	id   int64
	name string
}

type Station struct {
	id   int64
	name string
	code int64
}

type Route struct {
	id     int64
	dep_id int64
	arr_id int64
}

type TimeTable struct {
	id          int64
	route_id    int64
	dep         float64
	arr         float64
	time_travel float64
}

func parse(inp chan string) {
	for {
		url_ := Split(<-inp, " ")
		dep_id := url_[0]
		arr_id := url_[1]
		date := url_[2]
		url := "https://bilet.railways.kz/sale/default/route/search?_locale=kz&route_search_form%5BdepartureStation%5D=" + dep_id + "&route_search_form%5BarrivalStation%5D=" + arr_id + "&route_search_form%5BforwardDepartureDate%5D=" + date + "&route_search_form%5BbackwardDepartureDate%5D=&route_search_form%5BdayShift%5D%5B%5D=MORNING&route_search_form%5BdayShift%5D%5B%5D=DAY&route_search_form%5BdayShift%5D%5B%5D=EVENING&route_search_form%5BdayShift%5D%5B%5D=NIGHT&route_search_form%5BcarTypes%5D%5B%5D=1&route_search_form%5BcarTypes%5D%5B%5D=2&route_search_form%5BcarTypes%5D%5B%5D=3&route_search_form%5BcarTypes%5D%5B%5D=4&route_search_form%5BcarTypes%5D%5B%5D=5&route_search_form%5BcarTypes%5D%5B%5D=6"

		soup, err := goquery.ParseString(Request(url))
		if err != nil {
			panic(err)
		}
		items := soup.Find(".train.item").HtmlAll()
		for _, item := range items {
			item_q, _ := goquery.ParseString(item)
			result := ""

			// Получение и проверка название поезда

			train_name := item_q.Find(".train-information")
			t, ok := select_train(train_name.Attr("data-title"))
			t_id := t.id
			if !ok {
				_, err := insert_train(train_name.Attr("data-title"))
				if err != nil {
					panic(err)
				}
				t, _ := select_train(train_name.Attr("data-title"))
				t_id = t.id
			}

			result += " train_id: " + strconv.FormatInt(t_id, 10)

			// Путь

			route := item_q.Find(".ui.header.mobile.hidden.train-route")

			r, ok := select_route(dep_id, arr_id)
			route_id := r.id
			if !ok {
				_, err := insert_route(dep_id, arr_id)
				if err != nil {
					panic(err)
				}
				route_, _ := select_route(dep_id, arr_id)
				route_id = route_.id
			}
			result += " route_id: " + strconv.FormatInt(route_id, 10)

			// Время отправки

			dep_time, err := time.Parse(time.RFC3339, Join(Split(route.Attr("data-date"), " "), "T")+"+06:00") // Пример: 2020-01-26T10:23:00
			if err != nil {
				panic(err)
			}
			dep_timestamp := dep_time.UnixNano()
			result += " departure_time: " + strconv.FormatInt(dep_timestamp, 10)

			// Время в пути
			travel_q := item_q.Find(".center.aligned.column")
			travel := strings.Trim(Split(travel_q.Text(), "Жолда жүру уақыты")[1], " \n")
			if !strings.Contains(travel, ".") {
				travel = "0." + travel
			}
			hour_min_sec := Split(Split(travel, ".")[1], ":")
			days, _ := strconv.Atoi(Split(travel, ".")[0])
			hours, _ := strconv.Atoi(hour_min_sec[0])
			hms := strconv.Itoa(days*24+hours) + "h" + hour_min_sec[1] + "m" + hour_min_sec[2] + "s"
			dur, err := time.ParseDuration(hms)
			if err != nil {
				panic(err)
			}
			time_travel := dur.Nanoseconds()
			result += " time in travel: " + strconv.FormatInt(time_travel, 10)

			// Время прибытия = Время отправки + Время в пути

			arr_timestamp := dep_timestamp + time_travel
			result += " arr_time: " + strconv.FormatInt(arr_timestamp, 10)

			_, err = insert_timetable(route_id, dep_timestamp, arr_timestamp, time_travel)
			if err != nil {
				panic(err)
			}

			result += " arrival time: " + strconv.FormatInt(arr_timestamp, 10)

			timetable_, _ := select_timetable(route_id, dep_timestamp, arr_timestamp, time_travel)
			timetable_id := timetable_.id
			tp := ""
			pr := ""
			price, _ := goquery.ParseString(item_q.Find("table.ui.two.column.equal.width.very.basic.table.mobile.hidden").Html())
			price.Find(".ui.header").Each(
				func(_ int, element *goquery.Node) {

					for i := range element.Attr {
						if element.Attr[i].Key == "class" {
							if element.Attr[i].Val == "ui header" {
								tp = strings.Trim(element.Child[0].Data, " \n")
								result += " type: " + tp
							} else if element.Attr[i].Val == "ui apple header " {
								pr = strings.Trim(element.Child[0].Data, " \n")
								result += " price: " + pr
								price_n := Split(strings.Trim(pr, " \n₸"), ",")[0]
								price_tenge, err := strconv.Atoi(Join(Split(price_n, "\u00a0"), ""))
								if err != nil {
									panic(err)
								}
								_, err = insert_price(int64(timetable_id), tp, pr, price_tenge)
								if err != nil {
									panic(err)
								}
							}
						}
					}

				})

		}
		counter++
		wg.Done()
	}
}

func start() chan string { //функция вернет канал, из которого мы будем читать данные типа string
	inp := make(chan string)
	for i := 0; i < WORKERS; i++ { //в цикле создадим нужное нам количество гоурутин - worker'oв
		go parse(inp)
	}
	fmt.Println("Запущено потоков:", WORKERS)
	return inp
}

func ParserInit() {
	inp := start()
	wg.Add(1)
	go func() {
		for _, url := range urls {
			wg.Add(1)
			inp <- url
		}
		wg.Done()
	}()
}

func get_station(r *sql.Rows, _ error) []Station {
	ss := make([]Station, 0)
	for r.Next() {
		s := Station{}
		err := r.Scan(&s.id, &s.name, &s.code)
		if err != nil {
			panic(s)
		}
		ss = append(ss, s)
	}
	r.Close()
	return ss
}

func fill_urls() {
	today := time.Now()
	stations := get_station(db.Query("SELECT * FROM station"))
	stations2 := stations[:]

	for _, s1 := range stations {
		for _, s2 := range stations2 {
			url := strconv.FormatInt(s1.code, 10) + " " + strconv.FormatInt(s2.code, 10) + " " + today.Format("02-01-2006")
			urls = append(urls, url)
		}
		next_slice := len(stations2) - 2
		if next_slice > -1 {
			stations2 = stations2[:next_slice]
		}
	}

}

func main() {
	var err error
	db, err = sql.Open("sqlite3", "ktzh.db")
	if err != nil {
		panic(err)
	}
	defer db.Close()

	fill_urls()
	ParserInit()
	go func() {
		d, _ := time.ParseDuration("30s")
		for {
			time.Sleep(d)
			fmt.Println(time.Now().Format(time.RFC3339), "страниц просмотрено:", float64(counter)/float64(len(urls)))
		}
	}()
	wg.Wait()
	fmt.Println(counter)
}
