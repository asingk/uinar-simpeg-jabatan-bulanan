package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type DafPegResp struct {
	Data Data
}

type Data struct {
	DaftarPegawai DaftarPegawai
}

type DaftarPegawai struct {
	Count   int
	Pegawai []Pegawai
}

type Pegawai struct {
	ID               string
	Nama             string
	JenisJabatan     string
	StatusPegawai    StatusPegawai
	JabatanSaatIni   JabatanSaatIni
	UnitKerjaSaatIni []UnitKerjaSaatIni
	UnitGaji         UnitGaji
	UnitRemun        UnitRemun
	RiwayatPangkat   []RiwayatPangkat
}

type StatusPegawai struct {
	ID   int
	Nama string
}

type JabatanSaatIni struct {
	ID       string
	Level    Level
	SubLevel SubLevel
	Grade    Grade
}

type SsoRole struct {
	Code        string
	Description string
}

type Level struct {
	ID      string
	Nama    string
	SsoRole SsoRole
	Jabatan Jabatan
}

type Jabatan struct {
	ID   string
	Nama string
}

type SubLevel struct {
	ID   string
	Nama string
	//Grade Grade
}

type Grade struct {
	ID    string
	Remun int
}

type UnitKerjaSaatIni struct {
	ID          string
	UnitKerja   UnitKerja
	Bagian      Bagian
	Subbag      Subbag
	Posisi      Posisi
	Grade       Grade
	isSecondary bool
}

type Posisi struct {
	ID   string
	Nama string
}

type UnitKerja struct {
	ID   string
	Nama string
}

type Bagian struct {
	ID   string
	Nama string
}

type Subbag struct {
	ID   string
	Nama string
}

type UnitGaji struct {
	ID   string
	Nama string
}

type UnitRemun struct {
	ID   string
	Nama string
}

type RiwayatPangkat struct {
	GolonganRuang string
}

type BesaranRemun struct {
	ID                string
	StatusPegawaiId   int
	StatusPegawaiNama string
	JenisJabatan      string
	Persen            int
}

type UangMakan struct {
	ID       string
	Golongan string
	Jumlah   int
}

type Pajak struct {
	ID                string
	StatusPegawaiId   int
	StatusPegawaiNama string
	Golongan          string
	Persen            int
}

type JabatanBulanan struct {
	Nip               string
	Nama              string
	Golongan          string `bson:"golongan,omitempty"`
	IdStatusPegawai   int    `bson:"idStatusPegawai,omitempty"`
	NamaStatusPegawai string `bson:"namaStatusPegawai,omitempty"`
	JenisJabatan      string `bson:"jenisJabatan,omitempty"`
	UnitGaji          string `bson:"unitGaji,omitempty"`
	UnitRemun         string `bson:"unitRemun,omitempty"`
	Grade             string `bson:"grade,omitempty"`
	RemunGrade        int    `bson:"remunGrade,omitempty"`
	Jabatan           string `bson:"jabatan,omitempty"`
	Tahun             int
	Bulan             int
	ImplementasiRemun int `bson:"implementasiRemun,omitempty"`
	Pajak             int `bson:"pajak"`
	UangMakanHarian   int `bson:"uangMakanHarian,omitempty"`
}

var jabBulColl *mongo.Collection
var besRemColl *mongo.Collection
var pajakColl *mongo.Collection
var umColl *mongo.Collection
var ctx = context.TODO()
var currentTime = time.Now()
var jabBulPrevMonthList []interface{}
var jabBulThisMonthList []interface{}
var prevYear int
var prevMonth int

func init() {
	file, err := os.OpenFile("profiljabatanbulanan_"+strconv.Itoa(currentTime.Year())+strconv.Itoa(int(currentTime.Month()))+strconv.Itoa(currentTime.Day())+".log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatal(err)
	}
	log.SetOutput(file)
	if err := godotenv.Load(); err != nil {
		log.Println("No .env file found")
	}
	log.Println("connecting to db...")
	dbUrl := os.Getenv("MONGODB_URI")
	clientOptions := options.Client().ApplyURI(dbUrl)
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		log.Fatal(err)
	}

	err = client.Ping(ctx, nil)
	if err != nil {
		log.Fatal(err)
	}

	jabBulColl = client.Database("simpegNew").Collection("jabatanBulanan")
	besRemColl = client.Database("simpegNew").Collection("besaranRemun")
	pajakColl = client.Database("simpegNew").Collection("pajak")
	umColl = client.Database("simpegNew").Collection("uangMakan")
	if currentTime.Month() == 1 {
		prevYear = currentTime.Year() - 1
		prevMonth = 12
	} else {
		prevYear = currentTime.Year()
		prevMonth = int(currentTime.Month() - 1)
	}
}

func main() {
	log.Println("deleting prev month...")
	resultDelPrevMonth, err := jabBulColl.DeleteMany(ctx, bson.M{"tahun": prevYear, "bulan": prevMonth})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("DeleteMany removed %v document(s)\n", resultDelPrevMonth.DeletedCount)

	respDataPeg := getPeglist(0)
	fmt.Println("size:", len(respDataPeg.DaftarPegawai.Pegawai))
	fmt.Println("count:", respDataPeg.DaftarPegawai.Count)
	//fmt.Println(respDataPeg.DaftarPegawai.Count % len(respDataPeg.DaftarPegawai.Pegawai))
	loopRange := respDataPeg.DaftarPegawai.Count / len(respDataPeg.DaftarPegawai.Pegawai)
	if respDataPeg.DaftarPegawai.Count%len(respDataPeg.DaftarPegawai.Pegawai) > 0 {
		loopRange++
	}
	for i := 1; i < loopRange; i++ {
		getPeglist(i * 100)
	}

	// insert the bson object slice using InsertMany()
	log.Println("inserting prev month...")
	_, err = jabBulColl.InsertMany(ctx, jabBulPrevMonthList)
	// check for errors in the insertion
	if err != nil {
		panic(err)
	}
	// display the ids of the newly inserted objects
	//log.Println(resultsInsPrevMonth.InsertedIDs)

	// insert the bson object slice using InsertMany()
	log.Println("inserting this month...")
	_, err = jabBulColl.InsertMany(ctx, jabBulThisMonthList)
	// check for errors in the insertion
	if err != nil {
		panic(err)
	}
	// display the ids of the newly inserted objects
	//log.Println(resultsInsThisMonth.InsertedIDs)

}

func getPeglist(skip int) Data {
	log.Println("getting peg list... skip:", skip)
	url := os.Getenv("SIMPEG_GRAPHQL_URL")
	payload := strings.NewReader("{\"query\":\"query DaftarPegawai(\\n\\t$skip: Int\\n\\t$take: Int\\n\\t$orderBy: PegawaiOrderByInput\\n\\t$filter: PegawaiFilterInput\\n) {\\n\\tdaftarPegawai(skip: $skip, take: $take, orderBy: $orderBy, filter: $filter) {\\n\\t\\tcount\\n\\t\\tpegawai {\\n\\t\\t\\tid\\n\\t\\t\\tnama\\n\\t\\t\\tstatusPegawai{\\n\\t\\t\\t\\tid\\n\\t\\t\\t\\tnama\\n\\t\\t\\t}\\n\\t\\t\\tjabatanSaatIni{\\n\\t\\t\\t\\tid\\n\\t\\t\\t\\tlevel{\\n\\t\\t\\t\\t\\tid\\n\\t\\t\\t\\t\\tnama\\n\\t\\t\\t\\t\\tssoRole {\\n\\t\\t\\t\\t\\t\\tcode\\n\\t\\t\\t\\t\\t\\tdescription\\n\\t\\t\\t\\t\\t}\\n\\t\\t\\t\\t\\tjabatan{\\n\\t\\t\\t\\t\\t\\tid\\n\\t\\t\\t\\t\\t\\tnama\\n\\t\\t\\t\\t\\t}\\n\\t\\t\\t\\t}\\n\\t\\t\\t\\tsublevel{\\n\\t\\t\\t\\t\\tid\\n\\t\\t\\t\\t\\tnama\\n\\t\\t\\t\\t}\\n\\t\\t\\t\\tgrade{\\n\\t\\t\\t\\t\\tid\\n\\t\\t\\t\\t\\tremun\\n\\t\\t\\t\\t}\\n\\t\\t\\t}\\n\\t\\t\\tunitKerjaSaatIni{\\n\\t\\t\\t\\tid\\n\\t\\t\\t\\tunitKerja{\\n\\t\\t\\t\\t\\tid\\n\\t\\t\\t\\t\\tnama\\n\\t\\t\\t\\t}\\n\\t\\t\\t\\tbagian{\\n\\t\\t\\t\\t\\tid\\n\\t\\t\\t\\t\\tnama\\n\\t\\t\\t\\t}\\n\\t\\t\\t\\tsubbag{\\n\\t\\t\\t\\t\\tid\\n\\t\\t\\t\\t\\tnama\\n\\t\\t\\t\\t}\\n\\t\\t\\t\\tposisi{\\n\\t\\t\\t\\t\\tid\\n\\t\\t\\t\\t\\tnama\\n\\t\\t\\t\\t}\\n\\t\\t\\t\\tgrade{\\n\\t\\t\\t\\t\\tid\\n\\t\\t\\t\\t\\tremun\\n\\t\\t\\t\\t}\\n\\t\\t\\t\\tisSecondary\\n\\t\\t\\t}\\n\\t\\t\\tjenisJabatan\\n\\t\\t\\tunitGaji {\\n\\t\\t\\t\\tid\\n\\t\\t\\t\\tnama\\n\\t\\t\\t}\\n\\t\\t\\tunitRemun {\\n\\t\\t\\t\\tid\\n\\t\\t\\t\\tnama\\n\\t\\t\\t}\\n\\t\\t\\triwayatPangkat {\\n\\t\\t\\t\\tgolonganRuang\\n\\t\\t\\t}\\n\\t\\t}\\n\\t}\\n}\\n\",\"variables\":{\"filter\":{\"daftarStatusAktifId\":[1]},\"skip\":" + strconv.Itoa(skip) + "},\"operationName\":\"DaftarPegawai\"}")
	req, err := http.NewRequest("POST", url, payload)

	req.Header.Add("Content-Type", "application/json")
	apikey := os.Getenv("SIMPEG_GRAPHQL_APIKEY")
	req.Header.Add("apikey", apikey)

	res, err := http.DefaultClient.Do(req)

	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			log.Fatalf("Closing body failed: %s", err)
		}
	}(res.Body)
	body, err := io.ReadAll(res.Body)

	get := DafPegResp{}
	err = json.Unmarshal(body, &get)
	if err != nil {
		log.Fatalf("Reading body failed: %s", err)
	}

	for _, v := range get.Data.DaftarPegawai.Pegawai {
		log.Println(v)
		var golPeg string
		var pajak Pajak
		var uangMakan UangMakan
		if len(v.RiwayatPangkat) > 0 {
			golPeg = v.RiwayatPangkat[0].GolonganRuang
			golDepan := strings.Split(golPeg, "/")
			err = pajakColl.FindOne(ctx, bson.D{{"golongan", golDepan[0]}}).Decode(&pajak)
			err = umColl.FindOne(ctx, bson.D{{"golongan", golDepan[0]}}).Decode(&uangMakan)
		}
		grade := Grade{}
		var jabatan string
		var unitKerja string
		if v.JenisJabatan == "DT" || v.JabatanSaatIni.Level.SsoRole.Code == "STR" {
			for i := range v.UnitKerjaSaatIni {
				if !v.UnitKerjaSaatIni[i].isSecondary {
					// Found!
					grade = v.UnitKerjaSaatIni[i].Grade
					if v.UnitKerjaSaatIni[i].Subbag.ID != "" {
						unitKerja = v.UnitKerjaSaatIni[i].Subbag.Nama
					} else if v.UnitKerjaSaatIni[i].Bagian.ID != "" {
						unitKerja = v.UnitKerjaSaatIni[i].Bagian.Nama
					} else {
						unitKerja = v.UnitKerjaSaatIni[i].UnitKerja.Nama
					}
					jabatan = v.UnitKerjaSaatIni[i].Posisi.Nama + " " + unitKerja
					break
				}
			}
		} else {
			grade = v.JabatanSaatIni.Grade
			jabatan = v.JabatanSaatIni.Level.Jabatan.Nama + " " + v.JabatanSaatIni.Level.Nama + " " + v.JabatanSaatIni.SubLevel.Nama
		}

		var implementasiRemun BesaranRemun
		filter := bson.D{}
		if v.StatusPegawai.ID == 2 {
			filter = bson.D{{"statusPegawaiId", 2}}
		} else if v.JabatanSaatIni.Level.Jabatan.ID == "FUN" && strings.Contains(v.JabatanSaatIni.Level.Nama, "Calon") {
			if v.JabatanSaatIni.Level.SsoRole.Code == "DSN" {
				filter = bson.D{{"statusPegawaiId", 1}, {"jenisJabatan", "Cados"}}
			} else {
				filter = bson.D{{"statusPegawaiId", 1}, {"jenisJabatan", "Cafung"}}
			}
		} else {
			filter = bson.D{{"statusPegawaiId", v.StatusPegawai.ID}, {"jenisJabatan", v.JenisJabatan}}
		}
		if v.JenisJabatan != "" {
			err = besRemColl.FindOne(ctx, filter).Decode(&implementasiRemun)
		}
		if err != nil {
			fmt.Println(filter)
			if err == mongo.ErrNoDocuments {
				fmt.Println("No documents found")
			}
			panic(err)
		}

		jabPegPrevBul := JabatanBulanan{
			Nip:               v.ID,
			Nama:              v.Nama,
			Golongan:          golPeg,
			IdStatusPegawai:   v.StatusPegawai.ID,
			NamaStatusPegawai: v.StatusPegawai.Nama,
			JenisJabatan:      v.JenisJabatan,
			UnitGaji:          v.UnitGaji.ID,
			UnitRemun:         v.UnitRemun.ID,
			Grade:             grade.ID,
			RemunGrade:        grade.Remun,
			Jabatan:           jabatan,
			Tahun:             prevYear,
			Bulan:             prevMonth,
			ImplementasiRemun: implementasiRemun.Persen,
			Pajak:             pajak.Persen,
			UangMakanHarian:   uangMakan.Jumlah,
		}

		jabPegThisBul := JabatanBulanan{
			Nip:               v.ID,
			Nama:              v.Nama,
			Golongan:          golPeg,
			IdStatusPegawai:   v.StatusPegawai.ID,
			NamaStatusPegawai: v.StatusPegawai.Nama,
			JenisJabatan:      v.JenisJabatan,
			UnitGaji:          v.UnitGaji.ID,
			UnitRemun:         v.UnitRemun.ID,
			Grade:             grade.ID,
			RemunGrade:        grade.Remun,
			Jabatan:           jabatan,
			Tahun:             currentTime.Year(),
			Bulan:             int(currentTime.Month()),
			ImplementasiRemun: implementasiRemun.Persen,
			Pajak:             pajak.Persen,
			UangMakanHarian:   uangMakan.Jumlah,
		}

		jabBulPrevMonthList = append(jabBulPrevMonthList, jabPegPrevBul)
		jabBulThisMonthList = append(jabBulThisMonthList, jabPegThisBul)

	}

	return get.Data
}
