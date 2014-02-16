module Importer
  class Csv
    DATABASE_CONFIG = ActiveRecord::Base.connection_config
    GLOBAL_BUFFER_LENGTH = 'SET GLOBAL net_buffer_length=1000000;'
    GLOBAL_MAX_ALLOWED_PACKET = 'SET GLOBAL max_allowed_packet=1000000000;'
    CSV_FILE_NAME = 'tmp/csv_file.csv'
    require 'csv'

    def self.parse(csv_file_stream)
      return if csv_file_stream.blank?
      File.open(CSV_FILE_NAME, 'w+') { |f| f.write(csv_file_stream.read) }
      @errors = []
      @users_parsed_count = 0
      start_time = Time.now

      people_file_name, tag_file_name = self.build_sql_for_people_and_tags
      case DATABASE_CONFIG[:adapter]
      when /postgresql/
        # send system command to PostGRES DB to create people
        system( %(psql 'user=#{ DATABASE_CONFIG[:username] } #{ 'password=' + DATABASE_CONFIG[:password] unless DATABASE_CONFIG[:password].blank? } host=#{ DATABASE_CONFIG[:host] || 'localhost' } port=#{ DATABASE_CONFIG[:port] || '5432' } dbname=#{ DATABASE_CONFIG[:database] }' -f #{ people_file_name }) )
        # send system command to PostGRES DB to create tags
        system( %(psql 'user=#{ DATABASE_CONFIG[:username] } #{ 'password=' + DATABASE_CONFIG[:password] unless DATABASE_CONFIG[:password].blank? } host=#{ DATABASE_CONFIG[:host] || 'localhost' } port=#{ DATABASE_CONFIG[:port] || '5432' } dbname=#{ DATABASE_CONFIG[:database] }' -f #{ tag_file_name }) )
      when /mysql2/
        # send system command to MySQL DB to create people
        system( %(mysql --max_allowed_packet=2048M -u #{ DATABASE_CONFIG[:username] }  #{ '-p' + DATABASE_CONFIG[:password] unless DATABASE_CONFIG[:password].blank? } -D #{ DATABASE_CONFIG[:database] } -e "source #{ people_file_name }") )
        # send system command to MySQL DB to create tags
        system( %(mysql --max_allowed_packet=2048M -u #{ DATABASE_CONFIG[:username] }  #{ '-p' + DATABASE_CONFIG[:password] unless DATABASE_CONFIG[:password].blank? } -D #{ DATABASE_CONFIG[:database] } -e "source #{ tag_file_name }") )
      end

      sleep 0.1 until @users_parsed_count > (%x[wc -l 'tmp/csv_file.csv'].split(' ').first.to_i - 10)
      "  200001 tmp/csv_file.csv\n"

      taggings_file_name, people_datafile_name = self.build_sql_for_taggings_and_people_data
      case DATABASE_CONFIG[:adapter]
      when /postgresql/
        # send system command to PostGRES DB to create taggings join records
        system( %(psql 'user=#{ DATABASE_CONFIG[:username] } #{ 'password=' + DATABASE_CONFIG[:password] unless DATABASE_CONFIG[:password].blank? } host=#{ DATABASE_CONFIG[:host] || 'localhost' } port=#{ DATABASE_CONFIG[:port] || '5432' } dbname=#{ DATABASE_CONFIG[:database] }' -f #{ taggings_file_name }) )
        # send system command to PostGRES DB to create people datas
        system( %(psql 'user=#{ DATABASE_CONFIG[:username] } #{ 'password=' + DATABASE_CONFIG[:password] unless DATABASE_CONFIG[:password].blank? } host=#{ DATABASE_CONFIG[:host] || 'localhost' } port=#{ DATABASE_CONFIG[:port] || '5432' } dbname=#{ DATABASE_CONFIG[:database] }' -f #{ people_datafile_name }) )
      when /mysql2/
        # send system command to MySQL DB to create taggings join records
        system( %(mysql --max_allowed_packet=2048M -u #{ DATABASE_CONFIG[:username] }  #{ '-p' + DATABASE_CONFIG[:password] unless DATABASE_CONFIG[:password].blank? } -D #{ DATABASE_CONFIG[:database] } -e "source #{ taggings_file_name }") )
        # send system command to MySQL DB to create people datas
        system( %(mysql --max_allowed_packet=2048M -u #{ DATABASE_CONFIG[:username] }  #{ '-p' + DATABASE_CONFIG[:password] unless DATABASE_CONFIG[:password].blank? } -D #{ DATABASE_CONFIG[:database] } -e "source #{ people_datafile_name }") )
      end

      @errors = ['No errors, YAY!'] if @errors.blank?
      end_time = Time.now
      return @errors, @users_parsed_count, @users_parsed_count+(@errors.size-1), File.open(CSV_FILE_NAME, 'r').readline, (end_time - start_time)
    end

    private

    def self.build_sql_for_people_and_tags
      case DATABASE_CONFIG[:adapter]
      when /mysql2/
        people_file_name = 'tmp/people_mysql.sql'
        tag_file_name = 'tmp/tags_mysql.sql'
        File.open(people_file_name, 'w+') { |f| f.write( GLOBAL_BUFFER_LENGTH + GLOBAL_MAX_ALLOWED_PACKET + 'INSERT IGNORE INTO people (id,' )}
        File.open(tag_file_name, 'w+') { |f| f.write( GLOBAL_BUFFER_LENGTH + GLOBAL_MAX_ALLOWED_PACKET + 'INSERT IGNORE INTO tags (id,' )}
        values_sql_beginning = "('','"
      when /postgresql/
        people_file_name = 'tmp/people_postgres.sql'
        tag_file_name = 'tmp/tags_postgres.sql'
        File.open(people_file_name, 'w+') { |f| f.write( 'INSERT INTO people (' )}
        File.open(tag_file_name, 'w+') { |f| f.write( 'INSERT INTO tags (' )}
        values_sql_beginning = "('"
      end
      time = Time.now.strftime('%Y-%m-%d %I:%M:%S')
      people = self.build_people_hash
      all_tag_ids_by_tag_name = self.all_tag_ids_by_tag_name
      people_values = []
      tag_values = []

      CSV.foreach(CSV_FILE_NAME, { headers: :first_row, skip_blanks: :true }) { |csv_row|
        people_values << values_sql_beginning + csv_row['email'] + "','" + time + "','" + time + "')" if !csv_row['email'].blank? || people['email'].blank?
        @users_parsed_count += 1

        if !csv_row.blank? && !csv_row['tags'].blank? && all_tag_ids_by_tag_name[csv_row['tags']].blank?
          tag_values << csv_row['tags'].split(',').compact.map { |tag|
            values_sql_beginning + tag + "')"
          }
        end
      }

      if people_values.blank?
        File.delete(people_file_name)
        people_file_name = ''
      else
        File.open(people_file_name, 'a+') { |f|
          f.write('email,created_at,updated_at) VALUES ' + people_values.flatten.uniq.join(',') + ';')
        }
      end
      if tag_values.blank?
        File.delete(tag_file_name)
        tag_file_name = ''
      else
        File.open(tag_file_name, 'a+') { |f|
          f.write('name) VALUES ' + tag_values.flatten.uniq.join(',') + ';')
        }
      end
      return people_file_name, tag_file_name
    # rescue
    #   @errors << 'build_sql_for_people_and_tags failed to build SQL'
    end


    def self.build_sql_for_taggings_and_people_data
      case DATABASE_CONFIG[:adapter]
      when /mysql2/
        taggings_file_name = 'tmp/taggings_mysql.sql'
        people_datafile_name = 'tmp/people_data_mysql.sql'
        File.open(taggings_file_name, 'w+') { |f| f.write( GLOBAL_BUFFER_LENGTH + GLOBAL_MAX_ALLOWED_PACKET + 'INSERT IGNORE INTO taggings (id,' )}
        File.open(people_datafile_name, 'w+') { |f| f.write( GLOBAL_BUFFER_LENGTH + GLOBAL_MAX_ALLOWED_PACKET + %Q[INSERT IGNORE INTO people_data (id,\`key\`,\`value\`,] )}
        values_sql_beginning = "('','"
      when /postgresql/
        taggings_file_name = 'tmp/taggings_postgres.sql'
        people_datafile_name = 'tmp/people_data_postgres.sql'
        File.open(taggings_file_name, 'w+') { |f| f.write( 'INSERT INTO taggings (' )}
        File.open(people_datafile_name, 'w+') { |f| f.write( 'INSERT INTO people_data (key,value,' )}
        values_sql_beginning = "('"
      end
      time = Time.now.strftime('%Y-%m-%d %I:%M:%S')
      taggings_values = []
      people_data_values = []

      people = self.build_people_hash
      all_tag_ids_by_tag_name = self.all_tag_ids_by_tag_name

      CSV.foreach(CSV_FILE_NAME, { headers: :first_row, skip_blanks: :true }) { |csv_row|
        if !csv_row['tags'].blank?
          person_data = people[csv_row['email']]
          csv_row['tags'].split(',').compact.map { |tag_from_row|
            taggings_values << values_sql_beginning + (all_tag_ids_by_tag_name[tag_from_row].to_s rescue 'NULL') + "','" + (person_data[:record].id.to_s rescue 'null') + "','Person','tags','" + time + "')"
          }

          csv_row.delete_if{ |f| f[0] == 'email' || f[0] == 'tags' }.map { |field|
            if field[0].blank? || field[1].blank? || person_data.blank?
              next
            end
            people_data_values << values_sql_beginning + field[0].to_s + "','" + field[1].to_s + "','" + person_data[:record].id.to_s + "','" + time + "','" + time + "')"
          }
        end
      }

      taggings_values = taggings_values.uniq.join(',')
      people_data_values = people_data_values.uniq.join(',')

      if taggings_values.blank?
        File.delete(taggings_file_name)
        taggings_file_name = ''
      else
        File.open(taggings_file_name, 'a+') { |f|
          f.write( 'tag_id,taggable_id,taggable_type,context,created_at) VALUES ' + taggings_values + ';' )
        }
      end
      if people_data_values.blank?
        File.delete(people_datafile_name)
        people_datafile_name = ''
      else
        File.open(people_datafile_name, 'a+') { |f|
          # because this table has column names 'key' and 'value', we need to use the %Q[] string thing to enable proper escaping of \`
          f.write( 'person_id,created_at,updated_at) VALUES ' + people_data_values + ';' )
        }
      end
      return taggings_file_name, people_datafile_name
    # rescue
    #   @errors << 'build_sql_for_taggings_and_people_data failed to build SQL'
    end

    def self.build_people_hash
      results_hash = {}
      Person.all.each { |p|
        results_hash.merge!(
          { p.email =>
            { :record => p }
          }
        )
      }
      results_hash
    end

    def self.all_tag_ids_by_tag_name
      results_hash = {}
      Tag.all.each { |t|
        results_hash.merge!( { t.name => t.id } )
      }
      results_hash
    end

  end
end
