package com.github.hepeknet.generator.avro;

/**
 * Copied from https://github.com/linkedin/databus and modified
 *
 */
public interface RandomDataGenerator {
	public int getNextInt();

	public int getNextInt(int min, int max);

	public String getNextString();

	public String getNextString(int min, int max);

	public double getNextDouble();

	public float getNextFloat();

	public long getNextLong();

	public boolean getNextBoolean();

	public byte[] getNextBytes(int maxBytesLength);
}
