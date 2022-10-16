
BIBLE_INPUT_DIR=/user/hadoop/bible
SP_INPUT_DIR=/user/hadoop/shakespeare/shakespeare
OUTPUT_DIR_ROOT=/user/hadoop/out
JAR_BASE_DIR=~/asg1

ngc-bible:

	hadoop jar ${JAR_BASE_DIR}/ngc.jar NgramCount ${BIBLE_INPUT_DIR} ${OUTPUT_DIR_ROOT}/p3/bible


