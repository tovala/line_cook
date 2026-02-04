def fetch_single_result(cursor):
  output, = cursor.fetchone()
  return output