# source https://gist.github.com/norman/1535879

require 'pg'

conn = PG.connect( dbname: 'geosearch_test' )

p conn

conn.exec "CREATE EXTENSION IF NOT EXISTS cube"
conn.exec "CREATE EXTENSION IF NOT EXISTS earthdistance"
conn.exec "DROP TABLE IF EXISTS cities CASCADE"

conn.exec %q{
  CREATE TABLE cities(
    id         SERIAL NOT NULL PRIMARY KEY,
    name       VARCHAR(255) NOT NULL,
    state      CHAR(2) NOT NULL,
    population INTEGER NOT NULL DEFAULT 0,
    lat        DECIMAL(11,8) NOT NULL,
    lng        DECIMAL(11,8) NOT NULL
  )
}

conn.exec %q{
  CREATE OR REPLACE FUNCTION insert_city(_name text, _state text,
    _population integer, _lat decimal, _lng decimal) RETURNS VOID AS $$
    BEGIN
      INSERT INTO cities (name, state, population, lat, lng)
        VALUES (_name, _state, _population, _lat, _lng);
    END;
  $$ LANGUAGE 'plpgsql'
}

conn.exec "CREATE INDEX test_index ON cities USING gist (ll_to_earth(lat, lng))"


File.open("US.txt", "r:utf-8") do |file|
  i = 0;
  file.lines.each do |line|
    fields = line.strip.split("\t")

    # Check "feature class" field, only include populated places.
    next if fields[7] !~ /^PPL/

    data = {
      :name       => fields[1],
      :state      => fields[10].upcase,
      :population => fields[14].to_i,
      :lat        => fields[4].to_f,
      :lng        => fields[5].to_f,
    }
    i = i.next
    puts "#{i} - #{data[:name]}"
    conn.exec_params('SELECT insert_city($1, $2, $3, $4, $5)', [data[:name], data[:state], data[:population], data[:lat], data[:lng]])
    break if i == 100_000
  end
end