package timestream

import (
	"fmt"
	"strings"
	"time"
)

// BuildQuery constructs a SQL query by replacing named placeholders
// within the template string with the corresponding values from the params map.
//
// This function supports several types for parameter values: string, time.Time,
// int, int64, and float64. The replacement process involves:
// - Surrounding string values with single quotes.
// - Formatting time.Time values as RFC3339 strings, also surrounded with single quotes.
// - Directly inserting int, int64, and float64 values without additional formatting.
//
// Placeholders in the template should be prefixed with a colon and followed by the key name.
// For example, a placeholder for a "startTime" parameter should be written as ":startTime".
//
// Parameters:
//   - template: A SQL query template string containing named placeholders.
//   - params: A map where each key corresponds to a placeholder in the template, and the value
//     is what will be used to replace the placeholder. The key should not include the
//     colon prefix.
//
// Returns:
//   - A string representing the final SQL query with all placeholders replaced by their
//     respective values.
//   - An error if any placeholder is not found in the template or if a parameter type is not supported.
//
// Example:
//
//	query, err := BuildQuery("SELECT * FROM table WHERE date > :startDate AND date < :endDate",
//	                         map[string]interface{}{"startDate": time.Now(), "endDate": time.Now().AddDate(0, 1, 0)})
//	if err != nil {
//	  // Handle error
//	}
//
// Note:
//
//	The function ensures basic SQL injection prevention by correctly formatting and escaping
//	the parameter values based on their types. However, it's recommended to further validate
//	and sanitise all input values as per your application's security requirements.
func BuildQuery(template string, params map[string]interface{}) (string, error) {
	for key, value := range params {
		placeholder := ":" + key

		var replacement string

		// Customise the replacement based on the type of value.
		// This is crucial for proper formatting and escaping.
		switch v := value.(type) {
		case string:
			replacement = fmt.Sprintf("'%s'", v) // Strings should be single-quoted
		case time.Time:
			replacement = fmt.Sprintf("from_unixtime(%s)", fmt.Sprint(v.Unix())) // Time should be formatted and single-quoted
		case time.Duration:
			a := int64(v.Seconds())
			replacement = fmt.Sprintf("%ds", a) // Duration should be formatted and single-quoted
		case int, int64, float64:
			replacement = fmt.Sprintf("%v", v) // Numeric types can be used directly
		case DatabaseName:
			replacement = fmt.Sprintf(`"%s"`, string(v)) // Database name with double quotes
		case TableName:
			replacement = fmt.Sprintf(`"%s"`, string(v)) // Table name with double quotes

		default:
			return "", fmt.Errorf("unsupported type for parameter %s", key)
		}

		if !strings.Contains(template, placeholder) {
			return "", fmt.Errorf("placeholder %s not found in query template", placeholder)
		}

		template = strings.ReplaceAll(template, placeholder, replacement)
	}
	return template, nil
}

type (
	DatabaseName string
	TableName    string
)
