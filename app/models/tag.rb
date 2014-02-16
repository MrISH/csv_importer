class Tag < ActiveRecord::Base
  validates :name, uniqueness: :true
  attr_accessible :name

  has_and_belongs_to_many :tagging
  has_many :people, through: :taggings
end
