lint:
	find lib -iname '*.pm' -print0 | xargs -0 -n 1 -- perl -c --
	find t -iname '*.t' -print0 | xargs -0 -n 1 -- perl -c --
	for bin in bin/*; do perl -c $$bin || exit $$?; done
