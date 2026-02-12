require 'mkmf'

if enable_config('debug')
  $CFLAGS << ' -g -std=c99 -pedantic -Wall'
else
  $defs << '-DNDEBUG'
end
have_func('rb_safe_level', 'ruby.h')
have_func('rb_cData', 'ruby.h')
create_makefile('rbtree')
