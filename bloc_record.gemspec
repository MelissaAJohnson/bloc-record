Gem::Specification.new do |s|
   s.name          = 'bloc_record'
   s.version       = '0.0.1'
   s.date          = '2017-06-17'
   s.summary       = 'BlocRecord ORM'
   s.description   = 'An ActiveRecord-esque ORM adaptor'
   s.authors       = ['Melissa Johnson']
   s.email         = 'missyj2007@gmail.com'
   s.files         = Dir['lib/**/*.rb']
   s.require_paths = ["lib"]
   s.homepage      =
     'http://rubygems.org/gems/bloc_record'
   s.license       = 'MIT'
   s.add_runtime_dependency 'sqlite3', '~> 1.3'
   s.add_runtime_dependency 'pg', '~> 0.20.0'
   s.add_runtime_dependency 'activesupport'
 end
