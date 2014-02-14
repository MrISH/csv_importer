module Importer
  class Csv
    CSV_GROUP_SIZE = 5000
    require 'csv'

    def self.parse(csv_file_stream)
      puts "self.parse"
      return if csv_file_stream.blank?
      csv_file = csv_file_stream.read
      csv_row_groups = CSV.parse(csv_file).in_groups_of(CSV_GROUP_SIZE)
      csv_headers_to_return = csv_file.split("\n").first.split(',')
      database_config = ActiveRecord::Base.connection_config

      @errors = []
      @created_count = 0
      start_time = Time.now

      if csv_file.blank?
        @errors << 'You have not selected a file.'
        return @errors, @created_count, row_group.size, csv_headers_to_return, (Time.now - start_time)
      end

      if csv_headers_to_return.first != 'email'
        @errors << 'The first line must be field headers, not data.'
        return @errors, @created_count, row_group.size, csv_headers_to_return, (Time.now - start_time)
      end

      csv_row_groups.each do |row_group|
        row_group.compact!

        # People
        system(%(mysql -u #{database_config[:username]} -D #{database_config[:database]} < #{Rails.root + self.create_people_records(row_group.dup, csv_headers_to_return.dup)}))
        puts "PEOPLE::"

        # Tags
        system(%(mysql -u #{database_config[:username]} -D #{database_config[:database]} < #{Rails.root + self.create_tag_records(row_group.dup, csv_headers_to_return.dup)}))
        puts "TAGS::"

        people = Person.all

        # Taggings
        system(%(mysql -u #{database_config[:username]} -D #{database_config[:database]} < #{Rails.root + self.create_taggings_records(row_group.dup, csv_headers_to_return.dup, people)}))
        puts "TAGGINGS::"

        # PeopleData
        system(%(mysql -u #{database_config[:username]} -D #{database_config[:database]} < #{Rails.root + self.create_people_data_records(row_group.dup, csv_headers_to_return.dup, people)}))
        puts "PEOPLE_DATA::"

        puts "DONE!!"
      end

      @errors = ['No errors, YAY!'] if @errors.blank?
      end_time = Time.now

      return @errors, @created_count, @created_count+(@errors.size-1), csv_headers_to_return, (end_time - start_time)
    end

    private

    def self.create_people_records(p_csv_rows, pd_csv_headers)
      puts "self.create_people_records"
      file_name = 'tmp/people_query.txt'
      email_index = pd_csv_headers.find_index { |x| x == 'email' }
      people_query = ''
      values = []

      p_csv_rows.each do |csv_row|
        next if csv_row.first == 'email'

        if csv_row.size > pd_csv_headers.size
          @errors << "Row: #{csv_row} contained too many fields for the number of headers present."
          next
        end

        person_field = csv_row[email_index]
        values << %(('','#{person_field}'))
        @created_count += 1
      end
      values = values.uniq.join(',')
      people_query = %(INSERT IGNORE INTO people (id,email) VALUES #{values};)

      File.open(file_name, "w+") { |f| f.write(people_query) }
      file_name
    rescue
      @errors << 'create_people_records failed to build SQL'
    end

    def self.create_tag_records(t_csv_rows, t_csv_headers)
      puts "self.create_tag_records"
      file_name = 'tmp/tag_query.txt'
      email_index = t_csv_headers.find_index { |x| x == 'email' }
      tag_index = t_csv_headers.find_index { |x| x == 'tags' }
      t_csv_rows_2 =[]
      t_csv_rows.each { |r|
        t_csv_rows_2 << [
          r[email_index],
          r[tag_index]
        ]
      }
      t_csv_headers = ['email', 'tags']
      tags_query = ''
      values = []
      t_csv_rows_2.each do |email, tags|
        next if email == 'email'
        next if tags.blank?

        tags.split(',').each do |tag|
          next if tag.blank?
          values << %(('','#{tag}'))
        end
      end
      values = values.uniq.join(',')
      tags_query = %(INSERT IGNORE INTO tags (id,name) VALUES #{values};)

      File.open(file_name, "w+") { |f| f.write(tags_query) }
      file_name
    rescue
      @errors << 'create_tag_records failed to build SQL'
    end

    def self.create_taggings_records(tg_csv_rows, tg_csv_headers, people)
      puts "self.create_taggings_records"
      file_name = 'tmp/taggings_query.txt'
      all_tags = Tag.all
      taggings_query = ''
      email_index = tg_csv_headers.find_index { |x| x == 'email' }
      tag_index = tg_csv_headers.find_index { |x| x == 'tags' }
      tg_csv_rows_2 =[]
      tg_csv_rows.each { |r|
        tg_csv_rows_2 << [
          r[email_index],
          r[tag_index]
        ]
      }
      tg_csv_headers = ['email', 'tags']
      values = []

      tg_csv_rows_2.each do |email, tags|
        next if email == 'email'
        next if tags.blank?

        @person_id = people.select { |p| p.email == email }.first.id rescue 'NULL'
        tags.split(',').each do |tag_from_row|
          tag_id = all_tags.select { |t| t.name == tag_from_row }.first.id rescue 'NULL'
          values << %(('','#{tag_id}','#{@person_id}','Person','','','tags','#{Time.now.strftime("%Y-%m-%d %I:%M:%S")}'))
        end
      end

      values = values.uniq.join(',')
      taggings_query = %(INSERT IGNORE INTO taggings (id,tag_id,taggable_id,taggable_type,tagger_id,tagger_type,context,created_at) VALUES #{values};)

      File.open(file_name, "w+") { |f| f.write(taggings_query) }
      file_name
    rescue
      @errors << 'create_taggings_records failed to build SQL'
    end

    def self.create_people_data_records(pd_csv_rows, pd_csv_headers, people)
      puts "self.create_people_data_records"
      file_name = 'tmp/people_data_query.txt'
      # remove email and tag headers and fields so that key value pairs can be
      # dynamically created from the remaining columns across any and all imports
      tag_index = pd_csv_headers.find_index { |x| x == 'tags' }
      pd_csv_headers.delete_at(tag_index) unless tag_index.blank?
      email_index = pd_csv_headers.find_index { |x| x == 'email' }
      pd_csv_headers.delete_at(email_index) unless email_index.blank?
      people_data_query = ''
      values = []

      pd_csv_rows.each do |csv_row|
        next if csv_row.first == 'email'
        person_field = csv_row[email_index]
        @person_id = people.select { |p| p.email == person_field }.first.id rescue 'NULL'
        csv_row.delete_at(tag_index)
        csv_row.delete_at(email_index)

        if csv_row.size > pd_csv_headers.size
          @errors << "Row: #{csv_row} contained too many fields for the number of headers present."
          next
        end

        # because this table has column names 'key' and 'value', we need to use the %Q[] string thing to enable propper escaping of \`
        if !csv_row.blank?
          csv_row.each_with_index do |field, index|
            next if field.blank?
            values << %(('','#{pd_csv_headers[index]}','#{field}','#{@person_id}','#{Time.now.strftime("%Y-%m-%d %I:%M:%S")}','#{Time.now.strftime("%Y-%m-%d %I:%M:%S")}'))
          end
        end
      end
      values = values.uniq.join(',')
      people_data_query = %Q[INSERT IGNORE INTO people_data (id,\`key\`,\`value\`,person_id,created_at,updated_at) VALUES #{values};]

      File.open(file_name, "w+") { |f| f.write(people_data_query) }
      file_name
    rescue
      @errors << 'create_people_data_records failed to build SQL'
    end

  end
end
