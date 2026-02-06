def fetch_single_result(cursor):
  output, = cursor.fetchone()
  return output

def fetch_results_array(cursor):
  output = [x[0] for x in cursor.fetchall()]
  return output