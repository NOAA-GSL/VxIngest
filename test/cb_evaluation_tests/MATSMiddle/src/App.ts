declare var $: any;

enum LogLevel {
	DEBUG = 0, INFO, WARN, ERROR
}

class App {
	public static LOG_LEVEL = LogLevel.DEBUG;
	public static start = process.hrtime();


	public static log(loglevel: LogLevel, text: String) {
		if (undefined == loglevel || undefined == text) {
			return;
		}

		if (loglevel >= this.LOG_LEVEL) {
			switch (loglevel) {
				case LogLevel.DEBUG:
					console.log("DEBUG\t[" + App.timeStamp() + "]\t" + text);
					break;
				case LogLevel.INFO:
					console.info("INFO\t[" + App.timeStamp() + "]\t" + text);
					break;
				case LogLevel.WARN:
					console.warn("WARN\t[" + App.timeStamp() + "]\t" + text);
					break;
				case LogLevel.ERROR:
					console.error("ERROR\t[" + App.timeStamp() + "]\t" + text);
					break;
			}

		}
	}

	public static timeStamp() {
		var d = new Date();
		return (d.getHours() + ":" + d.getMinutes() + ":" + d.getSeconds()
			+ "." + d.getMilliseconds());
	}

	public static elapsed_time(note: any) {
		var precision = 3; // 3 decimal places
		var elapsed = process.hrtime(this.start)[1] / 1000000; // divide by a million to get nano to milli
		console.log(process.hrtime(this.start)[0] + " s, " + elapsed.toFixed(precision) + " ms - " + note); // print message + time
		this.start = process.hrtime(); // reset the timer
	}

}

export { LogLevel, App };
