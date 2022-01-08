package apache_beam_pso;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

//  https://en.wikipedia.org/wiki/Particle_swarm_optimization
//for each particle i = 1, ..., S do
//        Initialize the particle's position with a uniformly distributed random vector: xi ~ U(blo, bup)
//        Initialize the particle's best known position to its initial position: pi ← xi
//        if f(pi) < f(g) then
//        update the swarm's best known position: g ← pi
//        Initialize the particle's velocity: vi ~ U(-|bup-blo|, |bup-blo|)
//        while a termination criterion is not met do:
//        for each particle i = 1, ..., S do
//        for each dimension d = 1, ..., n do
//        Pick random numbers: rp, rg ~ U(0,1)
//        Update the particle's velocity: vi,d ← w vi,d + φp rp (pi,d-xi,d) + φg rg (gd-xi,d)
//        Update the particle's position: xi ← xi + vi
//        if f(xi) < f(pi) then
//        Update the particle's best known position: pi ← xi
//        if f(pi) < f(g) then
//        Update the swarm's best known position: g ← pi
public class pso {

    public static void main(String[] args) throws IOException {
        swarm_init();

//        for (int experiment_time = 0; experiment_time < 100; experiment_time++) {
//        double startTime = System.nanoTime();
//        //read swarm from file
//        ArrayList<particle> swarm = new ArrayList<>();
//        String file = "/share/word-count-beam/src/main/java/apache_beam_pso/pso_init_10000particle_1000dimension.txt";
//        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
//            for (String line; (line = br.readLine()) != null; ) {
//                particle p = particle.string_to_particle(line);
//                swarm.add(p);
//            }
//        }
//
//        //loop the swarm and update particle
//        int count = 1;
//        int swarm_size = swarm.size();
//
////                while a termination criterion is not met do:
//        for (int i = 0; i < count; i++) {
////        for each particle i = 1, ..., S do
//            for (int j = 0; j < swarm_size; j++) {
//                particle original_p = swarm.get(j);
//                //        for each dimension d = 1, ..., n do
//                //        Pick random numbers: rp, rg ~ U(0,1)s
//                //        Update the particle's velocity: vi,d ← w vi,d + φp rp (pi,d-xi,d) + φg rg (gd-xi,d)
//                particle.update_velocity(original_p);
//                //        Update the particle's position: xi ← xi + vi
//                particle.update_position(original_p);
//
//                //update--limit particle velocity and position
//                particle.limit_velocity_position(original_p);
//
//                original_p.fitness = particle.fitness_function(original_p.position);
//                //        if f(xi) < f(pi) then
//                //        Update the particle's best known position: pi ← xi
//                if (particle.compare_fitness(original_p.p_fitness, original_p.fitness)) {
//                    original_p.p_fitness = original_p.fitness;
//                    original_p.p_best = original_p.position.clone();
//                }
//
//                //        if f(pi) < f(g) then
//                //        Update the swarm's best known position: g ← pi
//                //Global topology: use class static variable to store the swam's best
////                if (particle.compare_fitness(particle.swarm_best_fitness, original_p.p_fitness)) {
////                    particle.swarm_best = original_p.p_best.clone();
////                    particle.swarm_best_fitness = original_p.p_fitness;
////                }
//
//                //update particle's best position
//                if (particle.compare_fitness(original_p.particle_best_fitness, original_p.p_fitness)) {
//                    original_p.particle_best = original_p.p_best.clone();
//                    original_p.particle_best_fitness = original_p.p_fitness;
//                }
//
//                //communicate with neighborhood
//                for (int k = 0; k < original_p.neighborhood.length; k++) {
//                    particle update_p = swarm.get(original_p.neighborhood[k]);
//                    if (particle.compare_fitness(original_p.particle_best_fitness, update_p.particle_best_fitness)) {
//                        //current particle compare with left particle's particle_best_fitness and update
//                        original_p.particle_best = update_p.particle_best.clone();
//                        original_p.particle_best_fitness = update_p.particle_best_fitness;
//                        swarm.set(j, original_p);
//                    }else if (particle.compare_fitness(update_p.particle_best_fitness, original_p.particle_best_fitness)) {
//                        //left particle compare with current particle's particle_best_fitness and update
//                        update_p.particle_best = original_p.particle_best.clone();
//                        update_p.particle_best_fitness = original_p.particle_best_fitness;
//                        swarm.set(original_p.neighborhood[k], update_p);
//                    }
//                }
//            }
////                //write result to files
////                String path = "/share/word-count-beam/src/main/java/apache_beam_pso/serial_result/serial_pso_output_100particle_2000iteration_30dimension_sphere"+i+".txt";
////                write_file(path, swarm, false);
//        }
//        //write the best fitness in the files
//        String path = "/share/word-count-beam/src/main/java/apache_beam_pso/serial_pso_output_10000particle_1iteration_1000dimension_sphere.txt";
//        write_file(path, swarm, false);
//
//        double endTime = System.nanoTime();
//        double duration = (endTime - startTime) / 1000000000;  //divide by 1000000 to get milliseconds.s
//        path = "/share/word-count-beam/src/main/java/apache_beam_pso/serial_pso_time_10000particle_1iteration_1000dimension_sphere.txt";
//        particle.write_file(path, duration,true);
//        System.out.println(duration);
//        }
    }

    /*
    * write swam into file
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

    /*
    * output the init PSO file
    * input: null
    * output: null
    */
    public static void swarm_init() throws IOException {
        //add all the particle into an array
        ArrayList<particle> swarm = new ArrayList<particle>();
        int swarm_size = 2000;
        int dimension = 200;
        for (int i = 0; i < swarm_size; i++) {
            particle p = new particle();
            p.id = i;
            p.position = new double[dimension];
            p.fitness = 0;
            p.p_best = new double[dimension];
            p.p_fitness = 0;
            p.velocity = new double[dimension];
            p.w = 0;
            p.rp = 0;
            p.rg = 0;

            //init array with feasible region boundary value
            double[] b_lower = new double[dimension];
            Arrays.fill(b_lower, -100);
            p.b_lower = b_lower;

            double[] b_upper = new double[dimension];
            Arrays.fill(b_upper, 100);
            p.b_upper = b_upper;

            p.swarm_size = swarm_size;

            //topology
//            p.neighborhood = new int[p.swarm_size];
            //global
//            for (int j = 0; j < p.swarm_size; j++) {
//                p.neighborhood[j] = j;
//            }

            p.neighborhood = new int[2];
            //Ring topology:set left and right particle id as neighbor
            for (int j = 0; j < 2; j++) {
                //left particle's id
                if (p.id == 0) {
                    //if the id is zero,use the end particle to the left
                    p.neighborhood[0] = swarm_size - 1;
                } else {
                    p.neighborhood[0] = p.id - 1;
                }
                //right particle's id
                p.neighborhood[1] = (p.id + 1) % swarm_size;
            }
            particle new_p = particle.particle_init(p);
            swarm.add(new_p);
        }
        //write result into file
        FileWriter writer = new FileWriter("/share/word-count-beam/src/main/java/apache_beam_pso/pso_init_sphere_2000particle_200dimension.txt");
        for (particle str : swarm) {
            writer.write(str + System.lineSeparator());
        }
        writer.close();
        System.out.println(particle.swarm_best_fitness);
    }
}
