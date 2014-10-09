/**
 * not used
 */
package edu.duke.cacheplanner.algorithm

import lpsolve.LpSolve
import lpsolve.LpSolveException
import scala.collection.mutable.StringBuilder


/**
 * @author mayuresh
 *
 */
class MaxMinBenefit {

  def solve = {
    val numQueues: Int = 3
    val numQueries: Array[Int] = Array(2, 2, 2)
    val colsPerQuery: Array[Array[Array[Int]]] = Array(Array(Array(1), Array(2)), 
        Array(Array(1), Array(2)), Array(Array(3), Array(3)))
//    val numDatasets: Int = 1	//add mapping from colnumbers to datasets
    val weights: Array[Double] = Array(1, 1, 1)
    val numCols: Int = 3
    val colSizes: Array[Int] = Array(10, 10, 10)
    val budget: Int = 20
    // Z, x_k's, q_ij's
    val numVariables: Int = 1 + numCols + numQueries.reduce(_+_)
    // per queue + per query per every column contained within + budget + per column
    val numConstraints: Int = numQueues + colsPerQuery.flatten.size + 1 + numCols
    
    try {
      val solver = LpSolve.makeLp(numConstraints, numVariables);

      // add constraints
      
      // constraint on benefit for every queue
      for (i <- 1 to numQueues) {
    	  var strQueueCon = new StringBuilder;
    	  strQueueCon ++= "1 ";	// Z
    	  for (k <- 1 to numCols) {
    	    strQueueCon ++= "0 ";	//x_k
    	  }
    	  for (i1 <- 1 to numQueues) {
    	    for (j <- 1 to numQueries(i1-1)) {
    	      if(i == i1) {
    	        strQueueCon ++= (-weights(i-1)) + " ";	//q_ij/W_i
    	      } else {
    	        strQueueCon ++= "0 ";
    	      }
    	    }
    	  }
    	  solver.strAddConstraint(strQueueCon.toString, LpSolve.LE, 0);
      }
      
      // constraint for benefit of every query
      for(i <- 1 to numQueues) {
        for(j <- 1 to numQueries(i-1)) {
    	  for(k <- colsPerQuery(i-1)(j-1)) {
    	    var strQueryCon = new StringBuilder;
    	    strQueryCon ++= "0 ";	//Z
    	    for(k1 <- 1 to numCols) { 
    	      if(k == k1) {
    	        strQueryCon ++= "-1 ";	//x_k
    	      } else {
    	        strQueryCon ++= "0 ";
    	      }
    	    }
    	    for(i1 <- 1 to numQueues) {
    	    	for(j1 <- 1 to numQueries(i1-1)) {
    	    		if(i == i1 && j == j1) {
    	    			strQueryCon ++= "1 ";	//q_ij
    	    		} else {
    	    			strQueryCon ++= "0 ";
    	    		}
    	    	}
    	    }
    	    solver.strAddConstraint(strQueryCon.toString, LpSolve.LE, 0);
    	  }
        }
      }
      
      // budget constraint
      var strBudgetCon = new StringBuilder;
      strBudgetCon ++= "0 ";	//Z
      for(k <- 1 to numCols) {
        strBudgetCon ++= colSizes(k-1) + " ";
      }
      for(i <- 1 to numQueues) {
        for(j <- 1 to numQueries(i-1)) {
          strBudgetCon ++= "0 ";	//q_ij
        }
      }
      solver.strAddConstraint(strBudgetCon.toString, LpSolve.LE, budget);
      
      // 0-1 constraint for every column
      for(k <- 1 to numCols) {
        var strColCon = new StringBuilder;
        strColCon ++= "0 ";	//Z
        for(k1 <- 1 to numCols) {
          if(k == k1) {
            strColCon ++= "1 ";	//x_k
          } else {
        	strColCon ++= "0 ";
          }
        }
        for(i <- 1 to numQueues) {
        	for(j <- 1 to numQueries(i-1)) {
        		strColCon ++= "0 ";	//q_ij
        	}
        }
        solver.strAddConstraint(strColCon.toString, LpSolve.LE, 1);
      }
      
      //Objective
      var strObj = new StringBuilder;
      strObj ++= "-1 ";	//Z
      for(k <- 1 to numCols) {
        strObj ++= "0 ";	//x_k
      }
      for(i <- 1 to numQueues) {
        for(j <- 1 to numQueries(i-1)) {
          strObj ++= "0 ";	//q_ij
        }
      }
      solver.strSetObjFn(strObj.toString);

//      for(k <- 1 to numCols) {
//        solver.setInt(k, true);
//      }

      // solve the problem
      solver.solve();

      // print solution
      System.out.println("Value of objective function: " + solver.getObjective());
      val variables = solver.getPtrVariables();
      for (i <- 0 to variables.length-1) {
        System.out.println("Value of var[" + i + "] = " + variables(i));
      }

      // delete the problem and free memory
      solver.deleteLp();
    }
    catch {case e: LpSolveException =>
       e.printStackTrace();
    }
      
  }
}