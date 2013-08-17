package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"time"
)

func merge(s ...[]string) []string {
	var full []string
	for _, y := range s {
		full = append(full, y...)
	}
	return full
}

func writeRow(c chan *Resource, w *csv.Writer, vw *csv.Writer) {
	propFields := []string{"property_id", "account_number", "full_address", "unit", "zip"}
	ownerFields := []string{"name", "street", "city", "state", "zip"}
	descFields := []string{"description", "beginning_point", "land_area", "improvement_area", "improvement_description", "exterior_condition", "zoning", "zoning_description", "building_code", "eq_id", "gma", "homestead"}
	salesFields := []string{"sales_date", "sales_price", "sales_type"}
	geomFields := []string{"x", "y"}

	valueFields := []string{"id", "certification_year", "assessment_date", "market_value_date", "market_value", "land_taxable", "land_exempt", "improvement_taxable", "improvement_exempt", "total_exempt", "exempt_code", "exempt_date", "exempt_description", "taxes", "certified"}

	header := merge(propFields, ownerFields, descFields, salesFields, geomFields)
	w.Write(header)
	vw.Write(append(valueFields, "opa_id"))

	for {
		select {
		case res := <-c:
			var f interface{}
			err := json.Unmarshal([]byte(res.Response), &f)
			if err != nil {
				fmt.Println(err)
			}
			j := f.(map[string]interface{})
			if j["status"] != "success" {
				fmt.Println(res.Id, res.Response)
				continue
			}

			d := j["data"].(map[string]interface{})
			p := d["property"].(map[string]interface{})
			own := p["ownership"].([]interface{})
			desc := p["characteristics"].(map[string]interface{})
			sale := p["sales_information"].(map[string]interface{})
			geom := p["geometry"].(map[string]interface{})

			rp := mapToSlice(p, propFields)
			ro := mapToSlice(own[0].(map[string]interface{}), ownerFields)
			rd := mapToSlice(desc, descFields)
			rs := mapToSlice(sale, salesFields)
			rg := mapToSlice(geom, geomFields)

			full := merge(rp, ro, rd, rs, rg)

			err = w.Write(full)
			if err != nil {
				fmt.Println(err)
			}
			w.Flush()

			// Write out valuations to another csv
			writeValuation(p["account_number"].(string), p["valuation_history"].([]interface{}),
				valueFields, vw)
		}
	}
}

func writeValuation(opaid string, vv []interface{}, fields []string, w *csv.Writer) {
	for _, v := range vv {
		vals := append(mapToSlice(v.(map[string]interface{}), fields), opaid)
		w.Write(vals)
	}
}

func mapToSlice(m map[string]interface{}, fields []string) []string {
	r := make([]string, len(fields))
	i := 0
	stringOrDate := func(input string) string {
		re := regexp.MustCompile("/Date\\((\\d*)-(\\d*)\\)")
		if re.MatchString(input) {
			subs := re.FindStringSubmatch(input)
			d, _ := time.ParseDuration("-4h")
			msEpoch, err := strconv.Atoi(subs[1])
			if err != nil {
				return input
			}
			return time.Unix(int64(msEpoch/1000), 0).Add(d).Format("2006-01-02")
		} else {
			return input
		}
	}

	for _, field := range fields {
		switch m[field].(type) {
		case string:
			r[i] = stringOrDate(m[field].(string))
		case int:
			r[i] = strconv.Itoa(m[field].(int))
		case float64:
			r[i] = strconv.FormatFloat(m[field].(float64), 'g', -1, 64)
		case bool:
			r[i] = strconv.FormatBool(m[field].(bool))
		case nil:
			r[i] = ""
		default:
			r[i] = ""
			fmt.Println("unknown type: ", field)
		}

		i++
	}

	return r
}
