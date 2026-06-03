Gem::Specification.new do |s|
  s.name = "rbtree"

  # Extract the version from README file
  s.version = File.read(File.join(__dir__, "README"))[/== Changes\n=== (\d+\.\d+\.\d+)/, 1] || raise("failed to detect the version from README")
  s.authors = ["OZAWA Takuma"]

  s.summary = "A sorted associative collection."
  s.description = <<END
A RBTree is a sorted associative collection that is implemented with a
Red-Black Tree. It maps keys to values like a Hash, but maintains its
elements in ascending key order. The interface is the almost identical
to that of Hash.
END
  s.homepage = "https://github.com/mame/rbtree"
  s.license = "MIT"
  s.required_ruby_version = Gem::Requirement.new(">= 1.8.6")

  s.require_paths = ["lib"]
  s.extensions = ["extconf.rb"]
  s.extra_rdoc_files = ["README", "rbtree.c"]
  s.files = File.foreach("MANIFEST").map {|s| s.chomp }
  s.rdoc_options = ["--title", "Ruby/RBTree", "--main", "README", "--exclude", "\\A(?!README|rbtree\\.c).*\\z"]
end
