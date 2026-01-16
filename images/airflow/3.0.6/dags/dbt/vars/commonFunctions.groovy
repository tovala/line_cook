def get_config(config_name) {
	def command = "aws ssm get-parameters --names /spice_rack/snowflake/$config_name --region us-west-2 --output text --with-decryption --query Parameters[*].Value"
	Process process = command.execute()
	def out = new StringBuffer()
	def err = new StringBuffer()
	process.consumeProcessOutput( out, err )
	process.waitFor()
	if( out.size() > 0 ) return out.toString().trim()
}