lint:
	find lib -iname '*.pm' -print0 | xargs -0 -n 1 -- perl -Ilib -c --
	find t -iname '*.t' -print0 | xargs -0 -n 1 -- perl -Ilib -c --
	for bin in bin/*; do perl -Ilib -c $$bin || exit $$?; done
