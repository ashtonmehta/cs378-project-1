package edu.utexas.cs.cs378;

import com.esotericsoftware.kryo.io.Input;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class Main {

	/**
	 * A main method to run examples.
	 *
	 * @param args not used
	 */
	public static void main(String[] args) {
		
		// Validate args (path to file and batch size)
		System.out.println("Validating input!");
		try {
			validateArgs(args);
		} catch (Exception e) {
			System.out.println("ERROR: " + e.getMessage());
			return;
		}

		// Extract args
		String file = Paths.get(args[0]).toAbsolutePath().toString();
		int batchSize = Integer.parseInt(args[1]);

		// Split up raw data into smaller files
		System.out.println("Splitting raw file into chunks!");
		generateTempFiles(file, batchSize);
		// Sort each of the smaller files
		System.out.println("Sorting individual chunk files!");
		sortTempFiles();
		// Merge sorted chunks into one file
		System.out.println("Merging chunks into sorted file!");
		mergeTempFiles();
	}

	/**
	 * @param args should contain path to
	 *             compressed input file
	 *             and batch size, where
	 *             batch size should fit
	 *             in memory
	 * */
	private static void validateArgs(String[] args) throws Exception {

		if (args.length != 2) {
			throw new Exception("FILEPATH and BATCH_SIZE needed as args");
		}

		Path inputFilePath = Paths.get(args[0]).toAbsolutePath();

		if (Files.notExists(inputFilePath)) {
			throw new Exception("FILEPATH does not exist");
		}

		int batchSize;
		try {
			batchSize = Integer.parseInt(args[1]);
		} catch (NumberFormatException e) {
			throw new Exception(e.getMessage());
		}

		if (batchSize < 1) {
			throw new Exception("BATCH_SIZE needs to be positive");
		}

	}

	private static void generateTempFiles(String inputFilePath, int batchSize) {
		int NUM_COMMAS_EXPECTED = 16;
		int FARE_AMT_INDEX = 11;
		String BASE_OUTPUT_FILE_PATH = "src/main/resources/unsorted/output.dat";

		// Set up serialize tool and output file
		Kryo kryo = new Kryo();
		kryo.register(SerializableObject.class);
		Output output = null;

		// Read large file, split into temp files
		try {
			FileInputStream fin = new FileInputStream(inputFilePath);
			BufferedInputStream bis = new BufferedInputStream(fin);
			// Here we uncompress .bz2 file
			CompressorInputStream input = new CompressorStreamFactory().createCompressorInputStream(bis);
			BufferedReader br = new BufferedReader(new InputStreamReader(input));
			// We keep track of any invalidLines we see
			// @todo write this into a separate file
			StringBuilder invalidLines = new StringBuilder();

			String line;
			long fileCount = 0;
			while ((line = br.readLine()) != null) {

				if (output == null || output.total() > batchSize) {
					if (output != null) {
						output.close();
					}

					String outputFilePath = BASE_OUTPUT_FILE_PATH.replace(".dat", "_" + fileCount + ".dat");
					output = new Output(new FileOutputStream(outputFilePath));
					fileCount++;
				}

				// validate line and extract fare amount
				String[] tokens = line.split(",");
				if (tokens.length != NUM_COMMAS_EXPECTED + 1) {
					invalidLines.append(line).append("\n");
					continue;
				}
				float fareAmount;
				try {
					fareAmount = Float.parseFloat(tokens[FARE_AMT_INDEX]);
				} catch (NumberFormatException e) {
					invalidLines.append(line).append("\n");
					continue;
				}

				kryo.writeObject(output, new SerializableObject(fareAmount, line));
			}

			fin.close();
			if (output != null) {
				output.close();
			}
			System.out.println(invalidLines.toString());

		} catch (Exception e) {
			System.out.println("ERROR: " + e.getMessage());
		}
	}

	private static void sortTempFiles() {
		String pathToUnsortedTempFiles = "src/main/resources/unsorted";

		Kryo kryo = new Kryo();
		kryo.register(SerializableObject.class);

		try {
			File folder = new File(pathToUnsortedTempFiles);
			for (File file : folder.listFiles()) {
				// reading in serialized objects
				String curPath = file.getAbsolutePath();
				Input input = new Input(new FileInputStream(curPath));
				ArrayList<SerializableObject> objects = new ArrayList<>();
				while (!input.end()) {
					objects.add(kryo.readObject(input, SerializableObject.class));
				}
				file.delete();
				input.close();

				// sorting objects and writing them back out
				Collections.sort(objects);
				String sortedOutputFilePath = curPath.replace("/unsorted", "/sorted");
				Output output = new Output(new FileOutputStream(sortedOutputFilePath));
				for (SerializableObject object : objects) {
					kryo.writeObject(output, object);
				}
				output.close();
			}
		} catch (Exception e) {
			System.out.println("ERROR: " + e.getMessage());
		}
	}

	private static void mergeTempFiles() {
		String pathToSortedTempFiles = "src/main/resources/sorted";
		Kryo kryo = new Kryo();
		kryo.register(SerializableObject.class);
		try {
			File folder = new File(pathToSortedTempFiles);
			// Add all file inputs to a data structure (size N)
			ArrayList<Input> fileInputs = new ArrayList<>();
			Map<Input, File> inputFileMap = new HashMap<>();

			for (File file : folder.listFiles()) {
				String curPath = file.getAbsolutePath();
				Input curInput = new Input(new FileInputStream(curPath));
				fileInputs.add(curInput);
				inputFileMap.put(curInput, file);
			}

			PriorityQueue<SerializableObject> pq = new PriorityQueue<>();
			for (int i = 0; i < fileInputs.size(); i++) {
				Input input = fileInputs.get(i);
				if (!input.end()) { // shouldnt be end bc its first line but just in case
					SerializableObject cur = kryo.readObject(input, SerializableObject.class);
					cur.setFileIndex(i);
					pq.add(cur);
				}
			}

			String finalOutputFilePath = "src/main/resources/SORTED_FILE_RESULT.txt";
			BufferedWriter writer = new BufferedWriter(new FileWriter(finalOutputFilePath));

			while (!pq.isEmpty()) {
				SerializableObject cur = pq.poll();
				writer.write(cur.getLine() + "\n");

				int inputIndex = cur.getFileIndex();
				Input curInput = fileInputs.get(inputIndex);

				if (curInput.end()) {
					curInput.close();
					File inputFile = inputFileMap.get(curInput);
					inputFile.delete();
					continue;
				}

				SerializableObject next = kryo.readObject(curInput, SerializableObject.class);
				next.setFileIndex(inputIndex);
				pq.add(next);
			}

			writer.close();

		} catch (Exception e) {
			System.out.println("ERROR: " + e.getMessage());
		}

	}
	
}