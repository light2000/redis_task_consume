################################################################################
# Automatically-generated file. Do not edit!
################################################################################

# Add inputs and outputs from these tool invocations to the build variables 
C_SRCS += \
./otm_task_monitor.c 

OBJS += \
./otm_task_monitor.o 

C_DEPS += \
./otm_task_monitor.d 


# Each subdirectory must supply rules for building sources it contributes
%.o: ./%.c
	@echo 'Building file: $<'
	@echo 'Invoking: GCC C Compiler'
	gcc -I/data/www/otm_online/otm/task_monitor/hiredis-master -O3 -Wall -c -fmessage-length=0 -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@)" -o "$@" "$<"
	@echo 'Finished building: $<'
	@echo ' '


