package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"os"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// https://developer.themoviedb.org/docs/daily-id-exports

const TMDB_APIKEY = ""
const MONGODB_CONNECTION = "mongodb://user:password@127.0.0.1:27017"
const MONGODB_DATABASE = ""
const MONGODB_COLLECTION = ""
const DATASET_FILE = "movie_ids_05_23_2024.json"

type State struct {
	SuccessCount      int      `json:"success_count"`
	FailureCount      int      `json:"failure_count"`
	Failed            []string `json:"failed"`
	RateLimitDetected bool
}

type DatasetRecord struct {
	ID            int     `json:"id"`
	OriginalTitle string  `json:"original_title"`
	Popularity    float64 `json:"popularity"`
	Video         bool    `json:"video"`
	Adult         bool    `json:"adult"`
}

func readJSONFile() ([]DatasetRecord, error) {
	file, err := os.Open(DATASET_FILE)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var records []DatasetRecord
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var record DatasetRecord
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			continue
		}
		if record.Adult {
			continue
		}
		records = append(records, record)
	}
	return records, nil
}

type Movie struct {
	GenreIds            []int         `json:"genre_ids" bson:"genre_ids"`
	Adult               bool          `json:"adult" bson:"adult"`
	BackdropPath        string        `json:"backdrop_path" bson:"backdrop_path"`
	BelongsToCollection interface{}   `json:"belongs_to_collection" bson:"belongs_to_collection"`
	Budget              int           `json:"budget" bson:"budget"`
	Genres              []interface{} `json:"genres" bson:"genres"`
	Homepage            string        `json:"homepage" bson:"homepage"`
	ID                  int           `json:"id" bson:"id"`
	ImdbID              string        `json:"imdb_id" bson:"imdb_id"`
	OriginalLanguage    string        `json:"original_language" bson:"original_language"`
	OriginalTitle       string        `json:"original_title" bson:"original_title"`
	Overview            string        `json:"overview" bson:"overview"`
	Popularity          float64       `json:"popularity" bson:"popularity"`
	PosterPath          string        `json:"poster_path" bson:"poster_path"`
	ProductionCompanies []interface{} `json:"production_companies" bson:"production_companies"`
	ProductionCountries []interface{} `json:"production_countries" bson:"production_countries"`
	ReleaseDate         string        `json:"release_date" bson:"release_date"`
	Revenue             int           `json:"revenue" bson:"revenue"`
	Runtime             int           `json:"runtime" bson:"runtime"`
	SpokenLanguages     []interface{} `json:"spoken_languages" bson:"spoken_languages"`
	Status              string        `json:"status" bson:"status"`
	Tagline             string        `json:"tagline" bson:"tagline"`
	Title               string        `json:"title" bson:"title"`
	Video               bool          `json:"video" bson:"video"`
	VoteAverage         float64       `json:"vote_average" bson:"vote_average"`
	VoteCount           int           `json:"vote_count" bson:"vote_count"`
	Videos              struct {
		Results []interface{} `json:"results" bson:"results"`
	} `json:"videos" bson:"videos"`
	Images struct {
		Backdrops []interface{} `json:"backdrops" bson:"backdrops"`
		Logos     []interface{} `json:"logos" bson:"logos"`
		Posters   []interface{} `json:"posters" bson:"posters"`
	} `json:"images" bson:"images"`
	Credits struct {
		Cast []interface{} `json:"cast" bson:"cast"`
		Crew []interface{} `json:"crew" bson:"crew"`
	} `json:"credits" bson:"credits"`
	Keywords struct {
		Keywords []interface{} `json:"keywords" bson:"keywords"`
	} `json:"keywords" bson:"keywords"`
}

func fetchMovie(client http.Client, id int) (*Movie, error) {
	data := &Movie{}
	response, err := client.Get(fmt.Sprintf("https://api.themoviedb.org/3/movie/%d?language=es-MX,es,en&append_to_response=videos,images,credits,keywords&include_video_language=es-MX,en-US&include_image_language=es-MX,en-US,null&api_key=%s", id, TMDB_APIKEY))
	if err != nil {
		return nil, err
	}
	if response.StatusCode == http.StatusTooManyRequests {
		return nil, errors.New("RateLimitExceededError")
	}
	if response.StatusCode == http.StatusNotFound {
		return nil, errors.New("NotFoundError")
	}
	if response.StatusCode != http.StatusOK {
		return nil, errors.New("UnexpectedError")
	}
	defer response.Body.Close()
	if err := json.NewDecoder(response.Body).Decode(&data); err != nil {
		return nil, err
	}
	return data, nil
}

func saveMovie(movie *Movie) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(MONGODB_CONNECTION))
	defer func() {
		if err = client.Disconnect(ctx); err != nil {
			fmt.Println(err.Error())
		}
	}()
	collection := client.Database(MONGODB_DATABASE).Collection(MONGODB_COLLECTION)
	_, err = collection.InsertOne(context.TODO(), movie)
	if err != nil {
		return err
	}
	return nil
	// os.MkdirAll("json-files/", 0755)
	// file, err := os.Create(fmt.Sprintf("json-files/%v.json", movie.ID))
	// if err != nil {
	// 	return err
	// }
	// defer file.Close()
	// encoder := json.NewEncoder(file)
	// encoder.SetIndent("", "  ")
	// if err := encoder.Encode(movie); err != nil {
	// 	return err
	// }
	// return nil
}

func saveState(state *State) error {
	file, err := os.Create("dump-state.json")
	if err != nil {
		return err
	}
	defer file.Close()
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(state); err != nil {
		return err
	}
	return nil
}

func processDatasetRecord(wg *sync.WaitGroup, client http.Client, record DatasetRecord, state *State) error {
	defer wg.Done()
	if state.RateLimitDetected {
		time.Sleep(time.Second)
	}
	movie, err := fetchMovie(client, record.ID)
	if err != nil {
		state.FailureCount++
		state.Failed = append(state.Failed, fmt.Sprintf("%d", record.ID))
		if err.Error() == "RateLimitExceededError" {
			state.RateLimitDetected = true
		}
		fmt.Printf("[Error] [ID=%d] %s --> %s\n", record.ID, record.OriginalTitle, err.Error())
		return err
	}
	if err := saveMovie(movie); err != nil {
		state.FailureCount++
		state.Failed = append(state.Failed, fmt.Sprintf("%d", record.ID))
		fmt.Printf("[Error] [ID=%d] %s --> %s\n", record.ID, record.OriginalTitle, err.Error())
		return err
	}
	state.SuccessCount++
	// fmt.Printf("[Success] [ID=%d] %s\n", record.ID, record.OriginalTitle)
	return nil
}

func main() {

	CHUNK_SIZE := 40
	SLEEP_TIME := 1 * time.Second
	OFFSET := 0
	LIMIT := 0 // CHUNK_SIZE * 5

	dataset, err := readJSONFile()
	if err != nil {
		panic(err)
	}
	var httpClient = &http.Client{Timeout: 10 * time.Second}
	fmt.Printf("%d records found.\n\n", len(dataset))

	var wg sync.WaitGroup

	state := &State{
		SuccessCount:      0,
		FailureCount:      0,
		Failed:            []string{},
		RateLimitDetected: false,
	}

	for index, datasetRecord := range dataset {
		if index >= LIMIT && LIMIT != 0 {
			break
		}
		if index+1 < OFFSET {
			continue
		}
		if state.RateLimitDetected {
			state.RateLimitDetected = false
			time.Sleep(SLEEP_TIME)
		}
		if index%CHUNK_SIZE == 0 {
			if index != 0 {
				time.Sleep(SLEEP_TIME)
			}
			fmt.Println("--------------------")

			totalRecords := len(dataset)
			currentChunk := (index / CHUNK_SIZE) + 1
			totalChunks := totalRecords / CHUNK_SIZE
			totalSeconds := totalChunks * int(SLEEP_TIME.Seconds())
			currentSeconds := currentChunk * int(SLEEP_TIME.Seconds())
			secondsLeft := totalSeconds - currentSeconds
			hours := int(math.Floor(float64(secondsLeft) / 3600))
			minutes := int(math.Floor(float64(secondsLeft)/60)) - hours*60
			seconds := secondsLeft % 60

			fmt.Printf("  Chunks: %d of %d.\n", currentChunk, totalChunks)
			fmt.Printf("  Records: %d of %d. %d errors.\n", state.SuccessCount, totalRecords, state.FailureCount)
			fmt.Printf("  Estimated time remaining: %d hours, %d minutes, %d seconds.\n", hours, minutes, seconds)
			fmt.Println("--------------------")
		}
		wg.Add(1)
		go processDatasetRecord(&wg, *httpClient, datasetRecord, state)
	}
	wg.Wait()
	saveState(state)
	fmt.Println("--------------------")
	fmt.Printf("  Saved %d of %d records. %d errors. See dump-state.json file for details.\n", state.SuccessCount, len(dataset), state.FailureCount)
	fmt.Println("--------------------")

}
