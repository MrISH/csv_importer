class CreatePeopleDatas < ActiveRecord::Migration
  def change
    create_table :people_data do |t|
      t.column :key, :string
      t.column :value, :string
      t.column :person_id, :integer
      t.timestamps
    end

    add_index :people_data, [:person_id, :key], :unique => true
  end
end
