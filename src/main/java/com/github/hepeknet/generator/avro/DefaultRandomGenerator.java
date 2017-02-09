package com.github.hepeknet.generator.avro;

import java.util.Random;

/**
 * Copied from https://github.com/linkedin/databus and modified
 *
 */
public class DefaultRandomGenerator implements RandomDataGenerator {

	private final Random rand;
	private static final String VALID_CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789"; // .,/';\\][=-`<>?\":|}{+_)(*&^%$#@!~";

	public DefaultRandomGenerator() {
		rand = new Random();
	}

	@Override
	public int getNextInt() {
		return getNextInt(0, IntegerFieldGenerate.getMaxIntLength());
	}

	@Override
	public int getNextInt(int min, int max) {
		if (max == min) {
			return min;
		}
		int range = max - min;
		range = range > 0 ? range : -range; // Prevent integer overflow.
		int generated = min + rand.nextInt(range);
		generated = generated > max ? max : generated;
		return generated;
	}

	@Override
	public String getNextString() {
		return getNextString(StringFieldGenerate.getMinStringLength(), StringFieldGenerate.getMaxStringLength());
	}

	@Override
	public String getNextString(int min, int max) {
		final int length = getNextInt(min, max);
		final StringBuilder strbld = new StringBuilder();
		for (int i = 0; i < length; i++) {
			final char ch = VALID_CHARACTERS.charAt(rand.nextInt(VALID_CHARACTERS.length()));
			strbld.append(ch);
		}
		return strbld.toString();
	}

	@Override
	public double getNextDouble() {
		return rand.nextDouble();
	}

	@Override
	public float getNextFloat() {
		return rand.nextFloat();
	}

	@Override
	public long getNextLong() {
		final long randomLong = rand.nextLong();
		return randomLong == Long.MIN_VALUE ? 0 : Math.abs(randomLong);
	}

	@Override
	public boolean getNextBoolean() {
		return rand.nextBoolean();
	}

	@Override
	public byte[] getNextBytes(int maxBytesLength) {
		final byte[] bytes = new byte[this.getNextInt(0, maxBytesLength)];
		rand.nextBytes(bytes);
		return bytes;
	}
}
