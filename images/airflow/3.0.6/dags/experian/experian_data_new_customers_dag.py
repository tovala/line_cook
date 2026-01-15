  @task()
  def get_token(token_url):
      """
      :param token_url: URL to generate token from Experian
      :return: authorization token for requests, valid for 30 minutes; None if response not returned
      """

      try:
        token_response = requests.post(
          url= 'https://us-api.experian.com/oauth2/v1/token',
          headers={
              'Content-Type': 'application/json',
          },
          data=json.dumps({
            'username': Variable.get('experian_username'),
            'password': Variable.get('experian_password'),
            'client_id': Variable.get('experian_client_id'),
            'client_secret': Variable.get('experian_client_secret')
          })
        )

        token_response.raise_for_status()
        token_response_json = token_response.json()
      
      except HTTPError as e:
        raise AirflowException(f'HTTP error occurred (updateZendeskTicketToClosed): {e}')
      except RequestException as e:
        raise AirflowException(f'Request error occurred (updateZendeskTicketToClosed): {e}')

      return token_response_json['access_token']