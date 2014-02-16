class PeopleData < ActiveRecord::Base

  belongs_to :person
  # validates :key, uniqueness: { scope: :person_id }

  attr_accessible :key, :value, :person_id



end
