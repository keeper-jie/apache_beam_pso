package apache_beam_pso;

import java.io.*;
import java.util.ArrayList;

public class parallel_pso implements Runnable {

    private Thread t;
    private final particle p;
    private final String threadName;

    parallel_pso(particle parameter_p, String parameter_threadName) {
        p = parameter_p;
        threadName = parameter_threadName;
    }

    @Override
    public void run() {
//        System.out.println("Running " + threadName);
//        System.out.println("Original p:" + p);
        //update velocity
        particle.update_velocity(p);
        //        Update the particle's position: xi ← xi + vi
        particle.update_position(p);
        //if the velocity and position beyond the range,reset it in range
        particle.limit_velocity_position(p);

        p.fitness = particle.fitness_function(p.position);
        //        if f(xi) < f(pi) then
        //        Update the particle's best known position: pi ← xi
        if (particle.compare_fitness(p.p_fitness, p.fitness)) {
            p.p_fitness = p.fitness;
            p.p_best = p.position.clone();
            //        if f(pi) < f(g) then
            //        Update the swarm's best known position: g ← pi
//            if (particle.compare_fitness(particle.swarm_best_fitness, p.p_fitness)) {
//                particle.swarm_best_fitness=p.p_fitness;
//                particle.swarm_best=p.p_best;
//            }
        }
        //update the particle's particle_best_fitness
        if (particle.compare_fitness(p.particle_best_fitness, p.p_fitness)) {
            p.particle_best_fitness = p.p_fitness;
            p.particle_best = p.p_best;
        }
//        System.out.println("update p:" + p);
//        System.out.println("Thread " + threadName + " exiting.");
    }

    public void start() {
//        System.out.println("Starting " + threadName);
        if (t == null) {
            t = new Thread(this, threadName);
            t.start();
        }
    }

    public static void main(String[] args) throws IOException {
        for (int experiment_time = 0; experiment_time < 10; experiment_time++) {
            double startTime = System.nanoTime();
            //read pso_init
            ArrayList<particle> swarm = new ArrayList<>();
            String file = "/share/word-count-beam/src/main/java/apache_beam_pso/pso_init.txt";
            try (BufferedReader br = new BufferedReader(new FileReader(file))) {
                for (String line; (line = br.readLine()) != null; ) {
                    particle p = particle.string_to_particle(line);
                    swarm.add(p);
                }
                // line is not visible here.
            }

            //loop the swarm and update particle
            int count = 500;
            int swarm_size = swarm.size();
            //        while a termination criterion is not met do:
            for (int i = 0; i < count; i++) {
//        for each particle i = 1, ..., S do
                for (int j = 0; j < swarm_size; j++) {
                    particle p = swarm.get(j);
                    parallel_pso R1 = new parallel_pso(p, "particle_" + j);
                    R1.start();
                    //store the updated particle_data
                    swarm.set(j, p);
                }
                //write result to file
                String path = "/share/word-count-beam/src/main/java/apache_beam_pso/parallel_pso_output.txt";
                write_file(path, swarm, false);
            }
//            System.out.println(particle.swarm_best_fitness);

            double endTime = System.nanoTime();
            double duration = (endTime - startTime) / 1000000000;  //divide by 1000000 to get milliseconds.
            String path = "/share/word-count-beam/src/main/java/apache_beam_pso/parallel_pso_time.txt";
            particle.write_file(path, duration);
            System.out.println(duration);
        }
    }

    public static void write_file(String path, ArrayList<particle> swarm, boolean is_append) throws IOException {
        FileWriter writer = new FileWriter(path, is_append);
        for (particle str : swarm) {
            writer.write(str + System.lineSeparator());
        }
        writer.close();
    }
}
