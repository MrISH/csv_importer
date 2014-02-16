class Tag < ActiveRecord::Base
  # validates :name, uniqueness: :true, :case_sensitive => false
  attr_accessible :name

  has_and_belongs_to_many :taggings
  has_many :people, through: :taggings
end
