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
        self.call_postgres(people_file_name)
        self.call_postgres(tag_file_name)
      when /mysql2/
        self.call_mysql(people_file_name)
        self.call_mysql(tag_file_name)
      end
      # used in self.build_people_hash && self.all_tag_ids_by_tag_name
      # because postgres has an issue finding all
      @person_count = Person.count
      @tag_count = Tag.count
      # wait until people records have been created/updated, as everything is related
      sleep 0.1 until Person.count >= @users_parsed_count

      taggings_file_name, people_data_file_name = self.build_sql_for_taggings_and_people_data
      case DATABASE_CONFIG[:adapter]
      when /postgresql/
        self.call_postgres(taggings_file_name)
        self.call_postgres(people_data_file_name)
      when /mysql2/
        taggings_file_name, people_data_file_name = self.build_sql_for_taggings_and_people_data
        self.call_mysql(taggings_file_name)
        self.call_mysql(people_data_file_name)
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

        tag_values << if !csv_row.blank? && !csv_row['tags'].blank? && all_tag_ids_by_tag_name[csv_row['tags']].blank?
          csv_row['tags'].split(',').compact.map { |tag|
            values_sql_beginning + tag + "')"
          }
        end
      }
      people_values = people_values.compact.uniq.join(',')
      tag_values = tag_values.flatten.compact.uniq.join(',')

      self.write_sql_file(people_file_name, 'email,created_at,updated_at) VALUES ', people_values )
      self.write_sql_file(tag_file_name, 'name) VALUES ', tag_values )
      return people_file_name, tag_file_name
    # rescue
    #   @errors << 'build_sql_for_people_and_tags failed to build SQL'
    end


    def self.build_sql_for_taggings_and_people_data
      case DATABASE_CONFIG[:adapter]
      when /mysql2/
        taggings_file_name = 'tmp/taggings_mysql.sql'
        people_data_file_name = 'tmp/people_data_mysql.sql'
        File.open(taggings_file_name, 'w+') { |f| f.write( GLOBAL_BUFFER_LENGTH + GLOBAL_MAX_ALLOWED_PACKET + 'INSERT IGNORE INTO taggings (id,' )}
        # because this table has column names 'key' and 'value', we need to use the %Q[] string thing to enable proper escaping of \`
        File.open(people_data_file_name, 'w+') { |f| f.write( GLOBAL_BUFFER_LENGTH + GLOBAL_MAX_ALLOWED_PACKET + %Q[INSERT IGNORE INTO people_data (id,\`key\`,\`value\`,] )}
        values_sql_beginning = "('','"
      when /postgresql/
        taggings_file_name = 'tmp/taggings_postgres.sql'
        people_data_file_name = 'tmp/people_data_postgres.sql'
        File.open(taggings_file_name, 'w+') { |f| f.write( 'INSERT INTO taggings (' )}
        File.open(people_data_file_name, 'w+') { |f| f.write( 'INSERT INTO people_data (key,value,' )}
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
            taggings_values << values_sql_beginning + (all_tag_ids_by_tag_name[tag_from_row].to_s rescue '') + "','" + (person_data[:record].id.to_s rescue '') + "','Person','tags','" + time + "')"
          }

          csv_row.delete_if{ |f| f[0] == 'email' || f[0] == 'tags' }.map { |field|
            if field[1] == '' || person_data.blank?
              next
            end
            people_data_values << values_sql_beginning + field[0].to_s + "','" + field[1].to_s + "','" + person_data[:record].id.to_s + "','" + time + "','" + time + "')"
          }
        end
      }

      taggings_values = taggings_values.flatten.compact.uniq.join(',')
      people_data_values = people_data_values.flatten.compact.uniq.join(',')

      self.write_sql_file(taggings_file_name, 'tag_id,taggable_id,taggable_type,context,created_at) VALUES ', taggings_values )
      self.write_sql_file(people_data_file_name, 'person_id,created_at,updated_at) VALUES ', people_data_values )
      return taggings_file_name, people_data_file_name
    # rescue
    #   @errors << 'build_sql_for_taggings_and_people_data failed to build SQL'
    end

    def self.build_people_hash
      results_hash = {}
      Person.find(:all, limit: @person_count).each { |p|
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
      Tag.find(:all, limit: @tag_count).each { |t|
        results_hash.merge!( { t.name => t.id } )
      }
      results_hash
    end

    def self.call_postgres(file_name)
      system( %(psql 'user=#{ DATABASE_CONFIG[:username] } #{ 'password=' + DATABASE_CONFIG[:password] unless DATABASE_CONFIG[:password].blank? } host=#{ DATABASE_CONFIG[:host] || 'localhost' } port=#{ DATABASE_CONFIG[:port] || '5432' } dbname=#{ DATABASE_CONFIG[:database] }' -f #{ file_name }) )
    end

    def self.call_mysql(file_name)
      system( %(mysql --max_allowed_packet=2048M -u #{ DATABASE_CONFIG[:username] } #{ '-p' + DATABASE_CONFIG[:password] unless DATABASE_CONFIG[:password].blank? } -D #{ DATABASE_CONFIG[:database] } < #{ file_name }) )
    end

    def self.write_sql_file(file_name, sql_column_names, values)
      if values.blank?
        File.delete(file_name)
        file_name = ''
      else
        File.open(file_name, 'a+') { |f|
          f.write(sql_column_names + values + ';')
        }
      end
    end

  end
end
