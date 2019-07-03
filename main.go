package main

// go get github.com/aws/aws-sdk-go
// go get -u github.com/gorilla/mux

// go run main.go /tmp/local-buckets

// https://github.com/aws/aws-sdk-go/blob/master/models/apis/s3/2006-03-01/api-2.json
// https://github.com/aws/aws-sdk-go/blob/master/service/s3/api.go
// https://docs.aws.amazon.com/sdk-for-go/api/service/s3/#ListObjectsInput

// add to etc/hosts 127.0.0.1    test-bucket.localhost

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/gorilla/mux"
)

const (
	contentTypeHeader  = "Content-Type"
	acceptHeader       = "Accept"
	applicationXML     = "application/xml"
	applicationJSON    = "application/json"
	separator          = "/"
	escapedSeparator   = "%2f"
	subdomainSeparator = "."
)

var localS3Path string

// TODO add requests validation

func main() {
	if len(os.Args) < 2 {
		panic("Path required as param")
	}

	localS3Path = os.Args[1]

	fmt.Println("Running")

	r := mux.NewRouter()

	r.Path("/").Queries("delete", "").HandlerFunc(deleteObjects).Methods("POST")
	r.Path("/").Queries("location", "").HandlerFunc(getBucketLocation).Methods("GET")
	r.HandleFunc("/", handleList).Methods("GET")
	r.HandleFunc("/", createBucket).Methods("POST")
	r.HandleFunc("/", deleteBucket).Methods("DELETE")
	r.HandleFunc("/{Key:.*}", putObject).Methods("PUT")
	r.HandleFunc("/{Key:.*}", getObject).Methods("GET")
	r.HandleFunc("/{Key:.*}", deleteObject).Methods("DELETE")

	http.Handle("/", r)
	log.Fatal(http.ListenAndServe(":8082", nil))
}

func handleList(w http.ResponseWriter, r *http.Request) {
	if strings.Contains(r.Host, subdomainSeparator) {
		listObjects(w, r)
	} else {
		listBuckets(w, r)
	}
}

//curl -X GET 'localhost:8080/'  | xmllint --format -
func listBuckets(w http.ResponseWriter, r *http.Request) {
	var result s3.ListBucketsOutput

	dir, _ := ioutil.ReadDir(localS3Path)

	for _, d := range dir {
		name := unescapeKey(d.Name())
		modTime := d.ModTime()
		bucket := s3.Bucket{Name: &name, CreationDate: &modTime}

		result.SetBuckets(append(result.Buckets, &bucket))
	}

	addResponse(r, w, result)

	log.Printf("List Bucket Objects: %v\n", result)
}

//curl -X GET 'localhost:8080/test-bucket?max-keys=2&marker=asd3'  | xmllint --format -
func listObjects(w http.ResponseWriter, r *http.Request) {
	bucket := getBucket(r)
	path := filepath.Join(localS3Path, bucket)

	if _, err := os.Stat(path); os.IsNotExist(err) {
		handleError(r, w, s3.ErrCodeNoSuchBucket, "", fmt.Sprintf("Bucket '%v' does not exists\n", bucket))

		return
	}

	var count int
	marker := r.FormValue("marker")
	prefix := r.FormValue("prefix")
	maxKeys, _ := strconv.Atoi(r.FormValue("max-keys"))
	delimiter := r.FormValue("delimiter")
	commonPrefixes := make(map[string]bool)
	re := getRegexpForList(delimiter, prefix)
	result := s3.ListObjectsOutput{Name: &bucket, Marker: &marker, Prefix: &prefix, Delimiter: &delimiter}

	result.SetMaxKeys(int64(maxKeys))
	files := getDirFilesFromMarker(path, &marker)
	var contentsSize int
	if maxKeys != 0 {
		contentsSize = maxKeys
	} else {
		contentsSize = len(files)
	}

	contents := make([]*s3.Object, 0, contentsSize)

	for _, d := range files {
		name := unescapeKey(d.Name())

		if prefix != "" && !strings.HasPrefix(name, prefix) {
			continue
		}

		if delimiter != "" {
			cp := re.FindStringSubmatch(name)
			if cp != nil {
				commonPrefixes[cp[1]] = true
				name = strings.Replace(name, cp[1], "", 1)
			}
		}

		if maxKeys != 0 && count >= maxKeys {
			result.SetIsTruncated(true)

			break
		}

		content := s3.Object{Key: &name}

		content.SetSize(d.Size())
		content.SetLastModified(d.ModTime())
		content.SetETag(strconv.FormatInt(d.ModTime().UnixNano(), 10))
		contents = append(contents, &content)

		count++
	}

	result.SetContents(contents)

	if delimiter != "" {
		result.SetCommonPrefixes(mapCommonPrefixesToResult(commonPrefixes))
	}

	addResponse(r, w, result)

	log.Printf("List Bucket Objects: %v\n", result)
}

func getRegexpForList(delimiter, prefix string) *regexp.Regexp {
	if delimiter != "" {
		delimRegexp := strings.Builder{}
		delimRegexp.WriteString("(")
		if prefix != "" {
			delimRegexp.WriteString(prefix)
		}
		delimRegexp.WriteString(".*")
		delimRegexp.WriteString(delimiter)
		delimRegexp.WriteString(").*")
		re, _ := regexp.Compile(delimRegexp.String())
		return re
	}

	return nil
}

func mapCommonPrefixesToResult(commonPrefixes map[string]bool) []*s3.CommonPrefix {
	var cp []*s3.CommonPrefix

	for key := range commonPrefixes {
		c := key
		cp = append(cp, &s3.CommonPrefix{Prefix: &c})
	}

	return cp
}

func getDirFilesFromMarker(path string, marker *string) []os.FileInfo {
	dir, _ := ioutil.ReadDir(path)

	var markerIndex int
	var result []os.FileInfo

	for i, d := range dir {
		result = append(result, d)

		if marker != nil && unescapeKey(d.Name()) == *marker {
			markerIndex = i
		}
	}

	return result[markerIndex:]
}

// curl -X GET localhost:8080/test-bucket?location
func getBucketLocation(w http.ResponseWriter, r *http.Request) {
	bucket := getBucket(r)
	path := filepath.Join(localS3Path, bucket)
	resourcePath := separator + bucket

	if _, err := os.Stat(path); os.IsNotExist(err) {
		handleError(r, w, s3.ErrCodeNoSuchBucket,
			"The requested bucket name is not available.",
			fmt.Sprintf("Bucket '%v' does not exists\n", resourcePath))

		return
	}

	location := r.Host + resourcePath

	addResponse(r, w, s3.GetBucketLocationOutput{LocationConstraint: &location})

	log.Printf("Get Bucket location: %v\n", path)
}

// curl -X POST localhost:8080/test-bucket -d '{"Buacket":"test-b"}' -i
func createBucket(w http.ResponseWriter, r *http.Request) {
	var body s3.CreateBucketInput

	if err := parseBody(r, w, r.Body, &body); err != nil {
		return
	}

	bucket := getBucket(r)
	path := filepath.Join(localS3Path, bucket)
	resourcePath := separator + bucket

	if _, err := os.Stat(path); os.IsNotExist(err) {
		os.Mkdir(path, os.ModePerm)
	} else {
		handleError(r, w, s3.ErrCodeBucketAlreadyExists,
			"The requested bucket name is not available. The bucket namespace is shared by all users of the system."+
				"Please select a different name and try again.",
			fmt.Sprintf("Bucket '%v' already exists\n", resourcePath))

		return
	}

	location := r.Host + resourcePath

	addResponse(r, w, s3.CreateBucketOutput{Location: &location})

	log.Printf("Created Bucket: %v\n", path)
}

// curl -X DELETE localhost:8080/test-bucket -i
func deleteBucket(w http.ResponseWriter, r *http.Request) {
	bucket := getBucket(r)
	path := filepath.Join(localS3Path, bucket)

	if _, err := os.Stat(path); err == nil {
		os.RemoveAll(path)
	}

	w.WriteHeader(http.StatusNoContent)
	log.Printf("Deleted Bucket: %v\n", bucket)
}

// curl -X PUT 'localhost:8080/test-bucket/test' --upload-file test  | xmllint --format -
func putObject(w http.ResponseWriter, r *http.Request) {
	// TODO validate content-length and content-md5
	vars := mux.Vars(r)
	path := filepath.Join(localS3Path, getBucket(r), escapeKey(vars["Key"]))

	if f, err := os.Create(path); err == nil {
		defer f.Close()
		io.Copy(f, r.Body)
	} else {
		handleError(r, w, "Failed saving file", "Failed saving file", "Failed saving file")

		log.Println(err)

		w.WriteHeader(http.StatusInternalServerError)
	}
}

func getObject(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	path := filepath.Join(localS3Path, getBucket(r), escapeKey(vars["Key"]))

	if f, err := ioutil.ReadFile(path); err == nil {
		w.Header().Add("ETag", "fba9dede5f27731c9771645a39863328")
		w.Header().Add("Content-Length", string(len(f)))
		w.Header().Add(contentTypeHeader, "text/plain")
		w.Write(f)
	} else {
		handleError(r, w, "Failed saving file", "Failed saving file", "Failed saving file")

		log.Println(err)

		w.WriteHeader(http.StatusInternalServerError)
	}
}

// curl -X DELETE localhost:8080/test-bucket/test -i
func deleteObject(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	path := filepath.Join(localS3Path, getBucket(r), escapeKey(vars["Key"]))
	result := s3.DeleteObjectOutput{}

	if _, err := os.Stat(path); err == nil {
		os.Remove(path)
		result.SetDeleteMarker(true)
	}

	w.WriteHeader(http.StatusNoContent)

	addResponse(r, w, result)

	log.Printf("Delete Object: %v\n", path)
}

// curl -X DELETE localhost:8080/test-bucket?delete -i
func deleteObjects(w http.ResponseWriter, r *http.Request) {
	bucket := getBucket(r)
	path := filepath.Join(localS3Path, bucket)
	dir, _ := ioutil.ReadDir(path)
	var deletedList []*s3.DeletedObject

	for _, d := range dir {
		name := unescapeKey(d.Name())

		os.RemoveAll(filepath.Join(path, name))

		deleted := s3.DeletedObject{}
		deleted.SetDeleteMarker(true)
		deleted.SetKey(name)

		deletedList = append(deletedList, &deleted)
	}

	result := s3.DeleteObjectsOutput{}
	result.SetDeleted(deletedList)

	addResponse(r, w, result)

	log.Printf("Delete Objects: %v\n", bucket)
}

// ******** Common Functions

func parseBody(r *http.Request, w http.ResponseWriter, body io.Reader, result interface{}) error {
	var err error

	switch r.Header.Get(contentTypeHeader) {
	case applicationJSON:
		err = json.NewDecoder(body).Decode(&result)
	case applicationXML:
		fallthrough
	default:
		err = xml.NewDecoder(body).Decode(&result)
	}

	if err != nil {
		handleError(r, w, "Failed to parse body", "Failed to parse body", "Failed to parse body")

		log.Println(err)

		return err
	}

	return nil
}

func handleError(r *http.Request, w http.ResponseWriter, errorCode string, errorMsg string, logMsg string) {
	w.WriteHeader(http.StatusBadRequest)
	addResponse(r, w, s3.Error{Code: &errorCode, Message: &errorMsg})
	log.Printf(logMsg)
}

func getBucket(r *http.Request) string {
	return strings.Split(r.Host, subdomainSeparator)[0]
}

func escapeKey(unescapedKey string) string {
	return strings.ReplaceAll(unescapedKey, separator, escapedSeparator)
}

func unescapeKey(escapedKey string) string {
	return strings.ReplaceAll(escapedKey, escapedSeparator, separator)
}

func addResponse(r *http.Request, w http.ResponseWriter, resp interface{}) {
	switch r.Header.Get(acceptHeader) {
	case applicationJSON:
		w.Header().Add(contentTypeHeader, applicationJSON)
		json.NewEncoder(w).Encode(resp)
	case applicationXML:
		fallthrough
	default:
		w.Header().Add(contentTypeHeader, applicationXML)
		xml.NewEncoder(w).Encode(resp)
	}
}

