package apache_beam_pso;

import java.io.*;
import java.util.ArrayList;

/*
 * java PSO thread class
 */
public class parallel_pso implements Runnable {

    private Thread t;
    private final particle p;
    private final String threadName;

    /*
     * java thread constructor
     * input: null
     * output: null
     */
    parallel_pso(particle parameter_p, String parameter_threadName) {
        p = parameter_p;
        threadName = parameter_threadName;
    }

    /*
     * java thread run function
     * input: null
     * output: null
     */
    @Override
    public void run() {
//        System.out.println("Running " + threadName);
//        System.out.println("Original p:" + p);
        //update velocity
        particle.update_velocity(p);
        //Update the particle's position: xi ← xi + vi
        particle.update_position(p);
        //if the velocity and position beyond the range,reset it in range
        particle.limit_velocity_position(p);

        p.fitness = particle.fitness_function(p.position);
        //        if f(xi) < f(pi) then
        //        Update the particle's best known position: pi ← xi
        if (particle.compare_fitness(p.p_fitness, p.fitness)) {
            p.p_fitness = p.fitness;
            p.p_best = p.position.clone();

        }
        //update the particle's particle_best_fitness
        if (particle.compare_fitness(p.particle_best_fitness, p.p_fitness)) {
            p.particle_best_fitness = p.p_fitness;
            p.particle_best = p.p_best.clone();
        }
//        //        if f(pi) < f(g) then
//        //        Update the swarm's best known position: g ← pi
//        // update static swarm_best_fitness
//        if (particle.compare_fitness(particle.swarm_best_fitness, p.p_fitness)) {
//            particle.swarm_best_fitness = p.p_fitness;
//            particle.swarm_best = p.p_best.clone();
//        }
//        System.out.println("Update p:" + p);
//        System.out.println("Thread " + threadName + " exiting.");
    }

    /*
     * java thread start function
     * input: null
     * output: null
     */
    public void start() {
//        System.out.println("Starting " + threadName);
        if (t == null) {
            t = new Thread(this, threadName);
            t.start();
        }
    }

    /*
     * parallel PSO code
     * input: null
     * output: null
     */
    public static void main(String[] args) throws IOException {
//        for (int experiment_time = 0; experiment_time < 100; experiment_time++) {
        double startTime = System.nanoTime();
        //read pso_init
        ArrayList<particle> swarm = new ArrayList<>();
        String file = "/share/word-count-beam/src/main/java/apache_beam_pso/pso_init_generalized_griewank_100particle_30dimension.txt";
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            for (String line; (line = br.readLine()) != null; ) {
                particle p = particle.string_to_particle(line);
                swarm.add(p);
            }
        }

        //loop the swarm and update particle
        int count = 1;
        int swarm_size = swarm.size();
        //        while a termination criterion is not met do:
        for (int i = 0; i < count; i++) {
//        for each particle i = 1, ..., S do
            for (int j = 0; j < swarm_size; j++) {
                particle original_p = swarm.get(j);
                parallel_pso R1 = new parallel_pso(original_p, "particle_" + j);
                R1.start();
                swarm.set(j, original_p);
                //communicate with neighborhood
                for (int k = 0; k < original_p.neighborhood.length; k++) {
                    particle update_p = swarm.get(original_p.neighborhood[k]);
                    if (particle.compare_fitness(original_p.particle_best_fitness, update_p.particle_best_fitness)) {
                        //current particle compare with left particle's particle_best_fitness and update
                        original_p.particle_best = update_p.particle_best.clone();
                        original_p.particle_best_fitness = update_p.particle_best_fitness;
                        swarm.set(j, original_p);
                    }
                    if (particle.compare_fitness(update_p.particle_best_fitness, original_p.particle_best_fitness)) {
                        //left particle compare with current particle's particle_best_fitness and update
                        update_p.particle_best = original_p.particle_best.clone();
                        update_p.particle_best_fitness = original_p.particle_best_fitness;
                        swarm.set(original_p.neighborhood[k], update_p);
                    }
                }
//                    System.out.println("Communicate p:" + original_p);
            }
//                //write result to file
//                String path = "/share/word-count-beam/src/main/java/apache_beam_pso/parallel_result/parallel_pso_output_100particle_2000iteration_30dimension_sphere"+i+".txt";
//                write_file(path, swarm, false);
        }
        //write result to file
        String path = "/share/word-count-beam/src/main/java/apache_beam_pso/parallel_result/test_output.txt";
        write_file(path, swarm, false);

        double endTime = System.nanoTime();
        double duration = (endTime - startTime) / 1000000000;  //divide by 1000000 to get milliseconds.
        path = "/share/word-count-beam/src/main/java/apache_beam_pso/test_time.txt";
        particle.write_file(path, duration,true);
        System.out.println(duration);
//        }
    }

    /*
    * write swarm result into file
    * input: String path, ArrayList<particle> swarm, boolean is_append
    * output: null
    */
    public static void write_file(String path, ArrayList<particle> swarm, boolean is_append) throws IOException {
        FileWriter writer = new FileWriter(path, is_append);
        for (particle str : swarm) {
            writer.write(str + System.lineSeparator());
        }
        writer.close();
    }
}
