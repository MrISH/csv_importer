module Importer
  class Csv
    # CSV_GROUP_SIZE = 5000
    GLOBAL_BUFFER_LENGTH = 'SET GLOBAL net_buffer_length=1000000;'
    GLOBAL_MAX_ALLOWED_PACKET = 'SET GLOBAL max_allowed_packet=1000000000;'
    CSV_FILE_NAME = 'tmp/csv_file.csv'
    require 'csv'

    def self.parse(csv_file_stream)
      puts "self.parse"
      return if csv_file_stream.blank?
      File.open(CSV_FILE_NAME, 'w+') { |f| f.write(csv_file_stream.read) }
      csv_headers_to_return = File.open(CSV_FILE_NAME, 'r').readline.split(',')
      database_config = ActiveRecord::Base.connection_config
      @errors = []
      @created_count = 0
      start_time = Time.now

      people_file_name, tag_file_name = self.build_and_stuff
      system( %(mysql --max_allowed_packet=2048M -u #{ database_config[:username] }  #{ '-p' + database_config[:password] unless database_config[:password].blank? } -D #{ database_config[:database] } -e "source #{ people_file_name }") )
      system( %(mysql --max_allowed_packet=2048M -u #{ database_config[:username] }  #{ '-p' + database_config[:password] unless database_config[:password].blank? } -D #{ database_config[:database] } -e "source #{ tag_file_name }") )
      puts "::PEOPLE::&::TAGS"

      taggings_file_name, people_datafile_name = self.the_rest
      system( %(mysql --max_allowed_packet=2048M -u #{ database_config[:username] }  #{ '-p' + database_config[:password] unless database_config[:password].blank? } -D #{ database_config[:database] } -e "source #{ taggings_file_name }") )
      system( %(mysql --max_allowed_packet=2048M -u #{ database_config[:username] }  #{ '-p' + database_config[:password] unless database_config[:password].blank? } -D #{ database_config[:database] } -e "source #{ people_datafile_name }") )
      puts "::TAGGINGS::&::PEOPLE_DATA"

      puts "::DONE!!"

      @errors = ['No errors, YAY!'] if @errors.blank?
      end_time = Time.now

      return @errors, @created_count, @created_count+(@errors.size-1), csv_headers_to_return, (end_time - start_time)
    end

    private



    def self.build_and_stuff
      puts "self.build_and_stuff"
      people_file_name = 'tmp/people.sql'
      tag_file_name = 'tmp/tags.sql'
      File.open(people_file_name, 'w+') { |f| f.write(GLOBAL_BUFFER_LENGTH + GLOBAL_MAX_ALLOWED_PACKET) }
      File.open(tag_file_name, 'w+') { |f| f.write(GLOBAL_BUFFER_LENGTH + GLOBAL_MAX_ALLOWED_PACKET) }
      time = Time.now.strftime("%Y-%m-%d %I:%M:%S")
      people_values = []
      tag_values = []

      CSV.foreach(CSV_FILE_NAME, { headers: :first_row, skip_blanks: :true }) { |csv_row|
        people_values << "('','" + csv_row['email'] + "','" + time + "','" + time + "')"
        @created_count += 1

        tag_values << csv_row['tags'].split(',').compact.map { |tag|
          "('','" + tag + "')"
        } unless csv_row['tags'].blank?
      }

      File.open(people_file_name, 'a+') { |f|
        f.write('INSERT IGNORE INTO people (id,email,created_at,updated_at) VALUES' + people_values.uniq.join(',') + ';')
      }
      File.open(tag_file_name, 'a+') { |f|
        f.write('INSERT IGNORE INTO tags (id,name) VALUES' + tag_values.uniq.join(',') + ";\n")
      }
      return people_file_name, tag_file_name
    rescue
      @errors << 'create_people_records failed to build SQL'
    end


    def self.the_rest
      puts "self.the_rest"
      taggings_file_name = 'tmp/taggings.sql'
      people_datafile_name = 'tmp/people_data.sql'
      File.open(taggings_file_name, 'w+') { |f| f.write(GLOBAL_BUFFER_LENGTH + GLOBAL_MAX_ALLOWED_PACKET) }
      File.open(people_datafile_name, 'w+') { |f| f.write(GLOBAL_BUFFER_LENGTH + GLOBAL_MAX_ALLOWED_PACKET) }
      time = Time.now.strftime("%Y-%m-%d %I:%M:%S")
      taggings_values = []
      people_data_values = []

      people = self.build_people_hash

      all_tag_ids_by_tag_name = self.all_tag_ids_by_tag_name

      CSV.foreach(CSV_FILE_NAME, { headers: :first_row, skip_blanks: :true }) { |csv_row|
        person_data = people[csv_row['email']]
        taggings_values << csv_row['tags'].split(',').compact.map { |tag_from_row|
          "('','" + all_tag_ids_by_tag_name[tag_from_row].to_s + "','" + person_data[:record].id.to_s + "','Person','','','tags','" + time + "')"
        } unless csv_row['tags'].blank?

        people_data_values << csv_row.delete_if{ |f| f.first == 'email' || f.first == 'tags' }.each_with_index.map { |field, index|
          "('','" + field[0].to_s + "','" + field[1].to_s + "','" + person_data[:record].id.to_s + "','" + time + "','" + time + "')"
        }
      }

      File.open(taggings_file_name, 'a+') { |f|
        f.write('INSERT IGNORE INTO taggings (id,tag_id,taggable_id,taggable_type,tagger_id,tagger_type,context,created_at) VALUES' + taggings_values.uniq.join(',') + ';')
      }
      File.open(people_datafile_name, 'a+') { |f|
        # because this table has column names 'key' and 'value', we need to use the %Q[] string thing to enable proper escaping of \`
        f.write(%Q[INSERT IGNORE INTO people_data (id,\`key\`,\`value\`,person_id,created_at,updated_at) VALUES] + people_data_values.uniq.join(',') + ';')
      }
      return taggings_file_name, people_datafile_name
    rescue
      @errors << 'create_taggings_records failed to build SQL'
    end

    def self.build_people_hash
      people = {}
      Person.all.each { |p|
        people.merge!(
          { p.email =>
            { :record => p }
          }
        )
      }
      people
    end

    def self.all_tag_ids_by_tag_name
      all_tag_ids_by_tag_name = {}
      Tag.all.each { |t|
        all_tag_ids_by_tag_name.merge!( { t.name => t.id } )
      }
      all_tag_ids_by_tag_name
    end

  end
end
