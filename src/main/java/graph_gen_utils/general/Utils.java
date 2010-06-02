package graph_gen_utils.general;

import java.io.File;
import java.util.Stack;

public class Utils {

	public static String getTimeStr(long msTotal) {
		long ms = msTotal % 1000;
		long s = (msTotal / 1000) % 60;
		long m = (msTotal / 1000) / 60;

		return String.format("%d(m):%d(s):%d(ms)%n", m, s, ms);
	}

	public static void cleanDir(String path) {
		deleteDir(path);
		File dir = new File(path);
		dir.mkdir();
	}

	public static void deleteDir(String path) {
		File dir = new File(path);

		if (dir.exists() == false)
			return;

		Stack<File> dirStack = new Stack<File>();
		dirStack.push(dir);

		boolean containsSubFolder;
		while (!dirStack.isEmpty()) {
			File currDir = dirStack.peek();
			containsSubFolder = false;

			String[] fileArray = currDir.list();
			for (int i = 0; i < fileArray.length; i++) {
				String fileName = currDir.getAbsolutePath() + File.separator
						+ fileArray[i];
				File file = new File(fileName);
				if (file.isDirectory()) {
					dirStack.push(file);
					containsSubFolder = true;
				} else {
					file.delete(); // delete file
				}
			}

			if (!containsSubFolder) {
				dirStack.pop(); // remove curr dir from stack
				currDir.delete(); // delete curr dir
			}
		}

	}

}
