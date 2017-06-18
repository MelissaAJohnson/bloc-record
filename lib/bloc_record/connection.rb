require 'sqlite3'

module Connection
  def connection
    @connection ||= sqlite3::Database.new(BlocRecord.database_filename)
  end
end
