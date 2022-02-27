package com.github.benmanes.caffeine.cache.simulator.cache_mem_system;

import com.github.benmanes.caffeine.cache.simulator.Simulator;

public class SimRunner {

  public static void main(String[] args) {
  	// For running a single trace, call MyConfig.setTraceFileName ({trace-format:full-path-to-trace). 
  	// This allows running the program multiple times in parallel, each time with a different trace.
  	// For running concatenation of several traces, do NOT call MyConfig.setTraceFileName from here, 
  	// and instead, write the requested traces names within application.conf file.
	String tracesPath = "C:\\Users\\ofana\\Documents\\traces\\";
  	String runP8 	  = "arc:" 			 + tracesPath + "arc\\P8.lis";
  	String runP6 	  = "arc:" 			 + tracesPath + "arc\\P6.lis";
  	String runP3 	  = "arc:" 		 	 + tracesPath + "arc\\P3.lis";
  	String runF2 	  = "umass-storage:" + tracesPath + "umass\\storage\\F2.spc.bz2";
  	String runF1 	  = "umass-storage:" + tracesPath + "umass\\storage\\F1.spc.bz2";
  	String runScarab  = "scarab:" 		 + tracesPath + "scarab\\scarab.recs.trace.20160808T073231Z.xz";
  	String runWiki1   = "wikipedia:" 	 + tracesPath + "wiki\\wiki.1190448987.gz";
  	String runWiki2   = "wikipedia:" 	 + tracesPath + "wiki\\wiki2.1191403252.gz";
  	String runCorda	  = "corda:" 	     + tracesPath + "corda.trace_vaultservice.gz";
  	String runGradle  = "gradle:"        + tracesPath + "gradle\\gradle.build-cache.xz";
  	
 	MyConfig.setTraceFileName (runF1);
  
  	javax.swing.SwingUtilities.invokeLater(new Runnable() {
      	public void run() {
          	String[] args = {
          									"0", // number of first iteration. Used for multiple parallel simulations
          									"1", // number of iterations to run.
          									"" 	// Policies to run, splitted by periods, e.g.: "Lru.Frd". Suppoted policies are: "Lru", "Frd", "Hyperbolic", "WTinyLfu"
          									};
          	Simulator.main(args);
          
          }
      });
  }
}

	