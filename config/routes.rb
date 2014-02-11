CsvImporter::Application.routes.draw do
  root to: 'csv_importer#index'

  match 'csv_importer/import' => 'csv_importer#import'
  match 'csv_importer/results' => 'csv_importer#results'

end
