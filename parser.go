package main

import "C"

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/opesun/goquery"
)

var (
	WORKERS     int      = 2  // кол-во "потоков"
	DUP_TO_STOP int      = 20 // максимум повторов до останова
	DAYS        int      = 2  // количество дней для парсинга
	urls        []string = []string{}
)

func request(url string) string {
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

func zip(lists ...[]string) func() []string {
	zip := make([]string, len(lists))
	i := 0
	return func() []string {
		for j := range lists {
			if i >= len(lists[j]) {
				return nil
			}
			zip[j] = lists[j][i]
		}
		i++
		return zip
	}
}

func min(a, b int) int {
	if a > b {
		return b
	}
	return a
}

func parse(inp chan string, output chan string) {
	for {
		url := <-inp
		soup, err := goquery.ParseString(request(url))
		if err != nil {
			panic("Error in getting url: " + err.Error())
		}
		items := soup.Find(".train.item").HtmlAll()
		for _, item := range items {
			item_q, _ := goquery.ParseString(item)
			result := ""

			train_name := item_q.Find(".train-information")
			result += train_name.Attr("data-title")

			departure_time := item_q.Find(".departure-time")
			date_dep := strings.Split(departure_time.Text(), "\n")
			new_date_dep := make([]string, 0)
			for _, it := range date_dep {
				r := strings.TrimSpace(it)
				if r == "" {
					continue
				}
				new_date_dep = append(new_date_dep, r)
			}
			result += " " + strings.Join(new_date_dep[:3], " ")

			arrival_time := item_q.Find(".arrival-time")
			date_arr := strings.Split(arrival_time.Text(), "\n")
			new_date_arr := make([]string, 0)
			for _, it := range date_arr {
				r := strings.TrimSpace(it)
				if r == "" {
					continue
				}
				new_date_arr = append(new_date_arr, r)
			}
			result += " - " + strings.Join(new_date_arr[:3], " ")

			price, _ := goquery.ParseString(item_q.Find("table.ui.two.column.equal.width.very.basic.table.mobile.hidden").Html())
			types := price.Find(".sub.header").HtmlAll()
			prices := price.Find(".ui.apple.header").HtmlAll()

			for i := 0; i < min(len(types), len(prices)); i++ {
				tp, _ := goquery.ParseString(types[i])
				pr, _ := goquery.ParseString(prices[i])
				unclear := strings.Split(strings.Trim(tp.Text(), "\n "), " ")
				clear := make([]string, 0)
				for _, item := range unclear {
					if item == "\n" {
						continue
					}
					clear = append(clear, item)
				}

				result += " " + strings.Join(clear, "") + "-" + strings.Trim(pr.Text(), " \n")
			}

			output <- result
		}
	}
}

func start() (chan string, chan string) { //функция вернет канал, из которого мы будем читать данные типа string
	c := make(chan string)
	inp := make(chan string)
	for i := 0; i < WORKERS; i++ { //в цикле создадим нужное нам количество гоурутин - worker'oв
		go parse(inp, c)
	}
	fmt.Println("Запущено потоков:", WORKERS)
	return c, inp
}

//export ParserInit
func ParserInit(workers int, dup_to_stop int, days int) {
	WORKERS = workers
	DUP_TO_STOP = dup_to_stop
	DAYS = days
	today := time.Now()
	day, _ := time.ParseDuration("24h")
	for i := 0; i < DAYS; i++ {
		url := "https://bilet.railways.kz/sale/default/route/search?_locale=kz&route_search_form%5BdepartureStation%5D=2708001&route_search_form%5BarrivalStation%5D=2700000&route_search_form%5BforwardDepartureDate%5D=" + today.Format("02-01-2006") + "&route_search_form%5BbackwardDepartureDate%5D=&route_search_form%5BdayShift%5D%5B%5D=MORNING&route_search_form%5BdayShift%5D%5B%5D=DAY&route_search_form%5BdayShift%5D%5B%5D=EVENING&route_search_form%5BdayShift%5D%5B%5D=NIGHT&route_search_form%5BcarTypes%5D%5B%5D=1&route_search_form%5BcarTypes%5D%5B%5D=2&route_search_form%5BcarTypes%5D%5B%5D=3&route_search_form%5BcarTypes%5D%5B%5D=4&route_search_form%5BcarTypes%5D%5B%5D=5&route_search_form%5BcarTypes%5D%5B%5D=6"
		urls = append(urls, url)
		today = today.Add(day)
	}
	out, inp := start()
	go func() {
		for _, url := range urls {
			inp <- url
		}
	}()
	for i := 0; i < DUP_TO_STOP; i++ { //получаем 5 ответов и закругляемся
		fmt.Println(<-out)
	}
}

func main() {}
