count = 0
File.open(Rails.root + 'csv_import_4.csv', 'a+') { |f|
  1000.times do
    f << %(damien#{count += 1}@getvero.com,Damien,Brzoska,"tshirt#{count += 1},warm-lead",#{Time.now},Desktop\n)
  end
}
