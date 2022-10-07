for SIZE in 16 32 96 128 196
do
	convert -background none -resize ${SIZE}x${SIZE} icon.svg icon-${SIZE}x${SIZE}.png
done

for SIZE in 57 60 72 76 114 120 144 152
do
	convert -background none -resize ${SIZE}x${SIZE} icon.svg apple-touch-icon-${SIZE}x${SIZE}.png
done
